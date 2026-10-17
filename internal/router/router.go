package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Router manages dynamic reverse proxy routes via the Caddy Admin API.
// Each sandbox that exposes a port gets an HTTPS subdomain automatically.
// MCP exposure uses the same mechanism — just a different subdomain prefix.
//
// You already know this pattern from your CI/CD platform's Caddy dynamic routing.
type Router struct {
	caddyAdminURL  string
	platformDomain string
	httpClient     *http.Client
}

func New(caddyAdminURL, platformDomain string) *Router {
	return &Router{
		caddyAdminURL:  caddyAdminURL,
		platformDomain: platformDomain,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
	}
}

// RegisterPreviewURL creates an HTTPS subdomain that proxies to the sandbox pod.
// Called when a sandbox exposes a port via the network.expose config.
//
// Result: https://<sandboxID>.preview.<platformDomain>
func (r *Router) RegisterPreviewURL(sandboxID, podIP string, port int) (string, error) {
	subdomain := fmt.Sprintf("%s.preview.%s", sandboxID, r.platformDomain)
	upstream := fmt.Sprintf("%s:%d", podIP, port)

	if err := r.addRoute(sandboxID+"-preview", subdomain, upstream); err != nil {
		return "", fmt.Errorf("register preview url: %w", err)
	}
	return "https://" + subdomain, nil
}

// RegisterMCPEndpoint exposes a sandbox as an MCP server.
// AI agents connect to this URL and discover tools defined inside the sandbox.
//
// Result: https://mcp.<sandboxID>.sandbox.<platformDomain>
func (r *Router) RegisterMCPEndpoint(sandboxID, podIP string, mcpPort int) (string, error) {
	subdomain := fmt.Sprintf("mcp.%s.sandbox.%s", sandboxID, r.platformDomain)
	upstream := fmt.Sprintf("%s:%d", podIP, mcpPort)

	if err := r.addRoute(sandboxID+"-mcp", subdomain, upstream); err != nil {
		return "", fmt.Errorf("register mcp endpoint: %w", err)
	}
	return "https://" + subdomain, nil
}

// Deregister removes all routes for a sandbox — called on destroy.
func (r *Router) Deregister(sandboxID string) {
	r.deleteRoute(sandboxID + "-preview")
	r.deleteRoute(sandboxID + "-mcp")
}

// ── Caddy Admin API ───────────────────────────────────────────────────────────

// caddyRoute is the JSON structure Caddy expects for a route.
type caddyRoute struct {
	Handle []caddyHandle `json:"handle"`
	Match  []caddyMatch  `json:"match"`
	// ID is used to identify the route for later deletion
	Terminal bool `json:"terminal"`
}

type caddyHandle struct {
	Handler  string         `json:"handler"`
	Upstreams []caddyUpstream `json:"upstreams"`
}

type caddyUpstream struct {
	Dial string `json:"dial"`
}

type caddyMatch struct {
	Host []string `json:"host"`
}

// addRoute registers a new reverse proxy route via Caddy Admin API.
// POST /config/apps/http/servers/srv0/routes
func (r *Router) addRoute(routeID, subdomain, upstream string) error {
	route := caddyRoute{
		Match: []caddyMatch{{Host: []string{subdomain}}},
		Handle: []caddyHandle{{
			Handler: "reverse_proxy",
			Upstreams: []caddyUpstream{{Dial: upstream}},
		}},
		Terminal: true,
	}

	body, err := json.Marshal(route)
	if err != nil {
		return err
	}

	// Caddy Admin API — add a named route
	url := fmt.Sprintf("%s/config/apps/http/servers/srv0/routes", r.caddyAdminURL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("caddy api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("caddy api returned %d", resp.StatusCode)
	}
	return nil
}

// deleteRoute removes a route by ID from Caddy.
// DELETE /id/<routeID>
func (r *Router) deleteRoute(routeID string) error {
	url := fmt.Sprintf("%s/id/%s", r.caddyAdminURL, routeID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
