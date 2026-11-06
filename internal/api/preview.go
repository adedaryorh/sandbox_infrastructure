package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/pkg/models"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
)

func (h *Handler) Preview(c *gin.Context) {
	sb, e := h.getSandbox(c)
	if e != nil {
		return
	}
	if sb.Status != models.StatusRunning {
		c.JSON(409, gin.H{"error": "sandbox is not running"})
		return
	}
	port, e := strconv.Atoi(c.Param("port"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid port"})
		return
	}
	req, e := h.store.RuntimeConfig(c.Request.Context(), sb.ID)
	if e != nil {
		apiError(c, e)
		return
	}
	allowed := false
	if req.Network != nil {
		for _, p := range req.Network.Expose {
			if p.Port == port {
				allowed = true
			}
		}
	}
	if !allowed {
		c.JSON(404, gin.H{"error": "port is not exposed"})
		return
	}
	ip, e := h.k8s.GetPodIP(c.Request.Context(), sb.PodName)
	if e != nil || net.ParseIP(ip) == nil {
		c.JSON(503, gin.H{"error": "workload address unavailable"})
		return
	}
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort(ip, fmt.Sprint(port))}
	proxy := httputil.NewSingleHostReverseProxy(target)
	base := proxy.Director
	proxy.Director = func(r *http.Request) {
		base(r)
		r.URL.Path = c.Param("path")
		r.URL.RawPath = ""
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
		r.Header.Del("Authorization")
		r.Header.Del("Proxy-Authorization")
		r.Header.Del("Cookie")
		for key := range r.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-forwarded-") {
				r.Header.Del(key)
			}
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) { http.Error(w, "workload is unavailable", 502) }
	proxy.ServeHTTP(c.Writer, c.Request)
}
