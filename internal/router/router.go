package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/yourorg/sandboxing/pkg/models"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Router struct {
	admin, domain, scheme, upstream string
	client                          *http.Client
	mu                              sync.Mutex
}

func New(admin, domain string) *Router {
	return &Router{admin: strings.TrimRight(admin, "/"), domain: strings.Trim(domain, "."), scheme: "http", upstream: "host.docker.internal:8080", client: &http.Client{Timeout: 5 * time.Second}}
}
func (r *Router) SetScheme(s string) error {
	if s != "http" && s != "https" {
		return fmt.Errorf("preview scheme must be http or https")
	}
	r.scheme = s
	return nil
}
func (r *Router) RegisterPorts(id, ip string, ports []models.PortExpose) ([]string, error) {
	if net.ParseIP(ip) == nil {
		return nil, fmt.Errorf("invalid workload IP")
	}
	if _, e := url.Parse("http://" + r.domain); e != nil || strings.ContainsAny(r.domain, "/: @") {
		return nil, fmt.Errorf("invalid preview domain")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	children := []any{}
	urls := []string{}
	seen := map[int]bool{}
	for _, p := range ports {
		if p.Port < 1024 || p.Port > 65535 || seen[p.Port] {
			return nil, fmt.Errorf("invalid or duplicate preview port")
		}
		seen[p.Port] = true
		host := fmt.Sprintf("p%d.%s.preview.%s", p.Port, id, r.domain)
		children = append(children, map[string]any{"match": []any{map[string]any{"host": []string{host}}}, "handle": []any{map[string]any{"handler": "rewrite", "uri": "/v1/sandboxes/" + id + "/preview/" + fmt.Sprint(p.Port) + "{http.request.uri}"}, map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": r.upstream}}}}, "terminal": true})
		urls = append(urls, r.scheme+"://"+host)
	}
	group := map[string]any{"@id": "sandbox-" + id, "handle": []any{map[string]any{"handler": "subroute", "routes": children}}}
	body, _ := json.Marshal(group)
	// Named routes are replaced atomically on re-registration (e.g. after resume).
	code, e := r.request("PATCH", "/id/sandbox-"+id, body)
	if e != nil {
		return nil, e
	}
	if code == 404 {
		code, e = r.request("PUT", "/config/apps/http/servers/srv0/routes/0", body)
	}
	if e != nil {
		return nil, e
	}
	if code < 200 || code >= 300 {
		return nil, fmt.Errorf("Caddy rejected route: HTTP %d", code)
	}
	return urls, nil
}
func (r *Router) RegisterPreviewURL(id, ip string, port int) (string, error) {
	urls, e := r.RegisterPorts(id, ip, []models.PortExpose{{Port: port}})
	if e != nil {
		return "", e
	}
	return urls[0], nil
}
func (r *Router) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	code, e := r.request("DELETE", "/id/sandbox-"+id, nil)
	if e != nil {
		return e
	}
	if code != 404 && (code < 200 || code >= 300) {
		return fmt.Errorf("Caddy route deletion rejected: HTTP %d", code)
	}
	return nil
}
func (r *Router) request(method, path string, body []byte) (int, error) {
	req, e := http.NewRequest(method, r.admin+path, bytes.NewReader(body))
	if e != nil {
		return 0, e
	}
	req.Header.Set("Content-Type", "application/json")
	response, e := r.client.Do(req)
	if e != nil {
		return 0, e
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

func (r *Router) SetAPIUpstream(value string) error {
	if _, _, e := net.SplitHostPort(value); e != nil {
		return fmt.Errorf("PREVIEW_API_UPSTREAM must be host:port")
	}
	r.upstream = value
	return nil
}
