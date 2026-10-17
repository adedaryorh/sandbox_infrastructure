package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/yourorg/sandboxing/internal/audit"
	"github.com/yourorg/sandboxing/internal/k8s"
	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/internal/repl"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Handler holds all dependencies for the REST API layer.
type Handler struct {
	store        *store.DB
	orchestrator *orchestrator.Orchestrator
	k8s          *k8s.Client
	replManager  *repl.Manager
	webhooks     *webhook.Emitter
	auditor      *audit.Auditor
	wsUpgrader   websocket.Upgrader
}

func NewHandler(
	store *store.DB,
	orch *orchestrator.Orchestrator,
	k8s *k8s.Client,
	replManager *repl.Manager,
	webhooks *webhook.Emitter,
	auditor *audit.Auditor,
) *Handler {
	return &Handler{
		store:        store,
		orchestrator: orch,
		k8s:          k8s,
		replManager:  replManager,
		webhooks:     webhooks,
		auditor:      auditor,
		wsUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// RegisterRoutes mounts all routes on the Gin engine.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/v1")

	// ── Sandboxes ────────────────────────────────────────────────────────────
	sb := v1.Group("/sandboxes")
	sb.POST("", h.CreateSandbox)
	sb.GET("", h.ListSandboxes)
	sb.GET("/:id", h.GetSandbox)
	sb.DELETE("/:id", h.DestroySandbox)
	sb.POST("/:id/pause", h.PauseSandbox)
	sb.POST("/:id/resume", h.ResumeSandbox)

	// ── Execution ─────────────────────────────────────────────────────────────
	sb.POST("/:id/exec", h.Exec)
	sb.GET("/:id/exec/stream", h.ExecStream)

	// ── PTY ───────────────────────────────────────────────────────────────────
	// WebSocket — WS /v1/sandboxes/:id/pty
	sb.GET("/:id/pty", h.PTY)

	// ── Files ─────────────────────────────────────────────────────────────────
	sb.PUT("/:id/files/*path", h.WriteFile)
	sb.GET("/:id/files/*path", h.ReadFile)
	sb.DELETE("/:id/files/*path", h.DeleteFile)
	sb.GET("/:id/files", h.ListFiles)

	// ── REPL ──────────────────────────────────────────────────────────────────
	sb.POST("/:id/repls", h.OpenREPL)
	sb.POST("/:id/repls/:rid", h.SendREPL)
	sb.DELETE("/:id/repls/:rid", h.CloseREPL)

	// ── Snapshots ─────────────────────────────────────────────────────────────
	sb.POST("/:id/snapshots", h.CreateSnapshot)
	sb.GET("/:id/snapshots", h.ListSnapshots)

	// ── Checkpoints ───────────────────────────────────────────────────────────
	sb.POST("/:id/checkpoints", h.CreateCheckpoint)
	v1.POST("/checkpoints/:id/restore", h.RestoreCheckpoint)

	// ── MCP ───────────────────────────────────────────────────────────────────
	sb.POST("/:id/mcp/enable", h.EnableMCP)
	sb.GET("/:id/mcp", h.GetMCP)
	sb.DELETE("/:id/mcp", h.DisableMCP)

	// ── Metrics ───────────────────────────────────────────────────────────────
	sb.GET("/:id/metrics", h.GetMetrics)

	// ── Webhooks ──────────────────────────────────────────────────────────────
	v1.POST("/webhooks", h.CreateWebhook)
	v1.GET("/webhooks", h.ListWebhooks)
	v1.DELETE("/webhooks/:id", h.DeleteWebhook)

	// ── Secrets ───────────────────────────────────────────────────────────────
	v1.POST("/secrets", h.SetSecret)
	v1.GET("/secrets", h.ListSecrets)
	v1.DELETE("/secrets/:name", h.DeleteSecret)

	// ── Quota ─────────────────────────────────────────────────────────────────
	v1.GET("/quota", h.GetQuota)

	// ── Audit ─────────────────────────────────────────────────────────────────
	v1.GET("/audit", h.ListAudit)

	// ── Health ────────────────────────────────────────────────────────────────
	r.GET("/health", h.Health)
}

// ── Sandbox CRUD ──────────────────────────────────────────────────────────────

func (h *Handler) CreateSandbox(c *gin.Context) {
	var req models.CreateSandboxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tenantID := h.tenantID(c)
	sb, err := h.orchestrator.Create(c.Request.Context(), tenantID, &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, sb)
}

func (h *Handler) ListSandboxes(c *gin.Context) {
	tenantID := h.tenantID(c)
	limit := queryInt(c, "limit", 20)
	offset := queryInt(c, "offset", 0)

	sandboxes, err := h.store.ListSandboxes(c.Request.Context(), tenantID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"sandboxes": sandboxes, "limit": limit, "offset": offset})
}

func (h *Handler) GetSandbox(c *gin.Context) {
	sb, err := h.store.GetSandbox(c.Request.Context(), c.Param("id"))
	if err != nil {
		if err == store.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, sb)
}

func (h *Handler) DestroySandbox(c *gin.Context) {
	if err := h.orchestrator.Destroy(c.Request.Context(), h.tenantID(c), c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) PauseSandbox(c *gin.Context) {
	if err := h.orchestrator.Pause(c.Request.Context(), h.tenantID(c), c.Param("id")); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "paused"})
}

func (h *Handler) ResumeSandbox(c *gin.Context) {
	if err := h.orchestrator.Resume(c.Request.Context(), h.tenantID(c), c.Param("id")); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "resuming"})
}

// ── Exec ──────────────────────────────────────────────────────────────────────

// Exec runs a command inside the sandbox and returns the result — blocking.
func (h *Handler) Exec(c *gin.Context) {
	var req models.ExecRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	result, err := h.k8s.Exec(c.Request.Context(), sb.PodName, req.Cmd)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.auditor.Log(c.Request.Context(), "sandbox.exec", "api_key", h.tenantID(c), sb.ID,
		map[string]interface{}{"cmd": req.Cmd, "exit_code": result.ExitCode, "duration_ms": result.DurationMs},
		sb.Secrets,
	)

	c.JSON(http.StatusOK, result)
}

// ExecStream streams stdout/stderr as Server-Sent Events.
// Same pattern as your CI/CD platform's SSE log streaming.
func (h *Handler) ExecStream(c *gin.Context) {
	cmd := c.Query("cmd")
	if cmd == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cmd query param required"})
		return
	}

	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	// set SSE headers
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // disable nginx buffering

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported"})
		return
	}

	frames := make(chan models.ExecFrame, 64)

	go func() {
		h.k8s.ExecStream(c.Request.Context(), sb.PodName, cmd, frames)
		close(frames)
	}()

	for frame := range frames {
		data, _ := json.Marshal(frame)
		fmt.Fprintf(c.Writer, "data: %s\n\n", data)
		flusher.Flush()

		if frame.Type == "exit" {
			break
		}
	}
}

// ── PTY ───────────────────────────────────────────────────────────────────────

// PTY upgrades to WebSocket and opens a full interactive terminal inside the pod.
// The browser pairs this with xterm.js to render colours and handle input.
func (h *Handler) PTY(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	// upgrade HTTP → WebSocket
	conn, err := h.wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// open TTY stream into the pod
	ptyStream, err := h.k8s.OpenPTY(c.Request.Context(), sb.PodName)
	if err != nil {
		conn.WriteJSON(models.PTYFrame{Type: "exit", Code: 1})
		return
	}
	defer ptyStream.Close()

	// goroutine: pod stdout → WebSocket
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptyStream.Read(buf)
			if n > 0 {
				conn.WriteJSON(models.PTYFrame{
					Type: "output",
					Data: string(buf[:n]),
				})
			}
			if err != nil {
				conn.WriteJSON(models.PTYFrame{Type: "exit", Code: 0})
				return
			}
		}
	}()

	// main loop: WebSocket → pod stdin
	for {
		var frame models.PTYFrame
		if err := conn.ReadJSON(&frame); err != nil {
			break
		}
		switch frame.Type {
		case "input":
			ptyStream.Write([]byte(frame.Data))
		case "resize":
			ptyStream.Resize(frame.Cols, frame.Rows)
		}
	}
}

// ── Files ─────────────────────────────────────────────────────────────────────

func (h *Handler) WriteFile(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	path := c.Param("path")
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body: " + err.Error()})
		return
	}

	if err := h.k8s.WriteFile(c.Request.Context(), sb.PodName, path, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.auditor.Log(c.Request.Context(), "file.write", "api_key", h.tenantID(c), sb.ID,
		map[string]interface{}{"path": path, "bytes": len(data)}, nil)

	c.JSON(http.StatusOK, gin.H{"path": path, "bytes": len(data)})
}

func (h *Handler) ReadFile(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	data, err := h.k8s.ReadFile(c.Request.Context(), sb.PodName, c.Param("path"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data)
}

func (h *Handler) DeleteFile(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	if err := h.k8s.DeleteFile(c.Request.Context(), sb.PodName, c.Param("path")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) ListFiles(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	path := c.DefaultQuery("path", "/workspace")
	listing, err := h.k8s.ListDir(c.Request.Context(), sb.PodName, path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"path": path, "listing": listing})
}

// ── REPL ──────────────────────────────────────────────────────────────────────

func (h *Handler) OpenREPL(c *gin.Context) {
	var req struct {
		Kernel models.REPLKernel `json:"kernel" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}

	session, err := h.replManager.Open(c.Request.Context(), sb.ID, sb.PodName, req.Kernel)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, models.REPLSession{
		ID:        session.ID,
		SandboxID: sb.ID,
		Kernel:    session.Kernel,
		CreatedAt: session.CreatedAt,
	})
}

func (h *Handler) SendREPL(c *gin.Context) {
	var req models.REPLSendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.replManager.Send(c.Param("rid"), req.Code)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) CloseREPL(c *gin.Context) {
	if err := h.replManager.Close(c.Param("rid")); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

// ── Snapshots ─────────────────────────────────────────────────────────────────

func (h *Handler) CreateSnapshot(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	snap, err := h.orchestrator.Snapshot(c.Request.Context(), h.tenantID(c), c.Param("id"), req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, snap)
}

func (h *Handler) ListSnapshots(c *gin.Context) {
	snaps, err := h.store.ListSnapshots(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"snapshots": snaps})
}

// ── Checkpoints ───────────────────────────────────────────────────────────────

func (h *Handler) CreateCheckpoint(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ckpt, err := h.orchestrator.Checkpoint(c.Request.Context(), h.tenantID(c), c.Param("id"), req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, ckpt)
}

func (h *Handler) RestoreCheckpoint(c *gin.Context) {
	sb, err := h.orchestrator.RestoreCheckpoint(c.Request.Context(), h.tenantID(c), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, sb)
}

// ── MCP ───────────────────────────────────────────────────────────────────────

func (h *Handler) EnableMCP(c *gin.Context) {
	// MCP port runs inside the sandbox — default 8765
	// We register a Caddy route for it via the router package
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	mcpURL := fmt.Sprintf("https://mcp.%s.sandbox.yourplatform.com", sb.ID)
	h.store.UpdateMCPUrl(c.Request.Context(), sb.ID, mcpURL, true)
	c.JSON(http.StatusOK, gin.H{"url": mcpURL, "enabled": true})
}

func (h *Handler) GetMCP(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	c.JSON(http.StatusOK, gin.H{"url": sb.MCPUrl, "enabled": sb.MCPEnabled})
}

func (h *Handler) DisableMCP(c *gin.Context) {
	h.store.UpdateMCPUrl(c.Request.Context(), c.Param("id"), "", false)
	c.Status(http.StatusNoContent)
}

// ── Metrics ───────────────────────────────────────────────────────────────────

func (h *Handler) GetMetrics(c *gin.Context) {
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	// In production: query pod metrics-server or Prometheus
	c.JSON(http.StatusOK, gin.H{
		"sandbox_id": sb.ID,
		"status":     sb.Status,
		"uptime_sec": time.Since(sb.CreatedAt).Seconds(),
	})
}

// ── Webhooks ──────────────────────────────────────────────────────────────────

func (h *Handler) CreateWebhook(c *gin.Context) {
	var ep models.WebhookEndpoint
	if err := c.ShouldBindJSON(&ep); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ep.ID = fmt.Sprintf("wh-%d", time.Now().UnixNano())
	ep.TenantID = h.tenantID(c)
	if err := h.store.InsertWebhookEndpoint(c.Request.Context(), &ep); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, ep)
}

func (h *Handler) ListWebhooks(c *gin.Context) {
	// simplified — full implementation queries webhook_endpoints by tenant
	c.JSON(http.StatusOK, gin.H{"webhooks": []interface{}{}})
}

func (h *Handler) DeleteWebhook(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// ── Secrets ───────────────────────────────────────────────────────────────────

func (h *Handler) SetSecret(c *gin.Context) {
	var req struct {
		Name  string `json:"name" binding:"required"`
		Value string `json:"value" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// store in internal secrets table — never returned in API responses
	_, err := h.store.Raw().ExecContext(c.Request.Context(), `
		INSERT INTO secrets (name, value, tenant_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (name, tenant_id) DO UPDATE SET value=EXCLUDED.value`,
		req.Name, req.Value, h.tenantID(c),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// return name only — never the value
	c.JSON(http.StatusOK, gin.H{"name": req.Name, "created": true})
}

func (h *Handler) ListSecrets(c *gin.Context) {
	rows, _ := h.store.Raw().QueryContext(c.Request.Context(),
		`SELECT name FROM secrets WHERE tenant_id=$1`, h.tenantID(c))
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		names = append(names, name)
	}
	// values never included in response
	c.JSON(http.StatusOK, gin.H{"secrets": names})
}

func (h *Handler) DeleteSecret(c *gin.Context) {
	h.store.Raw().ExecContext(c.Request.Context(),
		`DELETE FROM secrets WHERE name=$1 AND tenant_id=$2`,
		c.Param("name"), h.tenantID(c))
	c.Status(http.StatusNoContent)
}

// ── Quota ─────────────────────────────────────────────────────────────────────

func (h *Handler) GetQuota(c *gin.Context) {
	tenantID := h.tenantID(c)
	usage, _ := h.store.GetQuotaUsage(c.Request.Context(), tenantID)
	limits, _ := h.store.GetQuotaLimits(c.Request.Context(), tenantID)
	c.JSON(http.StatusOK, gin.H{
		"usage":  usage,
		"limits": limits,
	})
}

// ── Audit ─────────────────────────────────────────────────────────────────────

func (h *Handler) ListAudit(c *gin.Context) {
	sandboxID := c.Query("sandbox_id")
	from := time.Now().AddDate(0, -1, 0) // default: last 30 days
	to := time.Now()

	if f := c.Query("from"); f != "" {
		from, _ = time.Parse(time.RFC3339, f)
	}
	if t := c.Query("to"); t != "" {
		to, _ = time.Parse(time.RFC3339, t)
	}

	entries, err := h.store.ListAuditEntries(c.Request.Context(), sandboxID, from, to, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

// ── Health ────────────────────────────────────────────────────────────────────

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now()})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (h *Handler) tenantID(c *gin.Context) string {
	if tid := c.GetString("tenant_id"); tid != "" {
		return tid
	}
	return "default" // replaced by real auth middleware in production
}

func (h *Handler) getSandbox(c *gin.Context) (*models.Sandbox, error) {
	sb, err := h.store.GetSandbox(c.Request.Context(), c.Param("id"))
	if err != nil {
		if err == store.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return nil, err
	}
	return sb, nil
}

func queryInt(c *gin.Context, key string, fallback int) int {
	if v := c.Query(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

// lineScanner helper for SSE — wraps any reader into line-by-line output.
func lineScanner(r io.Reader, out chan<- string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	close(out)
}
