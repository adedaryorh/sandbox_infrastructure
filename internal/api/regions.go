package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/pkg/models"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func RegionalCreation(local string, regions []orchestrator.Region, scheduler *orchestrator.RegionScheduler) (gin.HandlerFunc, error) {
	endpoints := map[string]*url.URL{}
	for _, r := range regions {
		u, e := url.Parse(r.Endpoint)
		if e != nil || u.Host == "" || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("region endpoints require explicit HTTPS origins")
		}
		endpoints[r.Name] = u
	}
	return func(c *gin.Context) {
		if c.Request.Method != "POST" || c.FullPath() != "/v1/sandboxes" {
			c.Next()
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2<<20)
		body, e := io.ReadAll(c.Request.Body)
		if e != nil {
			c.AbortWithStatusJSON(413, gin.H{"error": "request too large"})
			return
		}
		var request models.CreateSandboxRequest
		if json.Unmarshal(body, &request) != nil {
			c.AbortWithStatusJSON(400, gin.H{"error": "invalid JSON"})
			return
		}
		selected := request.Region
		if selected == "" || selected == "default" {
			selected = local
		}
		if selected == "auto" {
			selected = scheduler.PickRegion("auto")
		}
		if selected == local {
			request.Region = "default"
			encoded, _ := json.Marshal(request)
			c.Request.Body = io.NopCloser(bytes.NewReader(encoded))
			c.Request.ContentLength = int64(len(encoded))
			c.Header("X-Sandbox-Region", local)
			c.Next()
			return
		}
		target, ok := endpoints[selected]
		if !ok {
			c.AbortWithStatusJSON(400, gin.H{"error": "unknown region"})
			return
		}
		if c.GetHeader("X-Sandbox-Region-Hop") != "" {
			c.AbortWithStatusJSON(508, gin.H{"error": "region forwarding loop rejected"})
			return
		}
		request.Region = selected
		encoded, _ := json.Marshal(request)
		c.Request.Body = io.NopCloser(bytes.NewReader(encoded))
		c.Request.ContentLength = int64(len(encoded))
		c.Request.Header.Set("X-Sandbox-Region-Hop", "1")
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
			http.Error(w, "regional control plane unavailable", 502)
		}
		c.Header("X-Sandbox-Region", selected)
		proxy.ServeHTTP(c.Writer, c.Request)
		c.Abort()
	}, nil
}
