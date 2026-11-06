package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"time"
)

// Stateless MCP JSON-RPC transport supporting the 2025-11-25 protocol.
func (h *Handler) MCP(c *gin.Context) {
	if !sameOrigin(c.Request) {
		c.JSON(403, gin.H{"error": "origin rejected"})
		return
	}
	sb, err := h.getSandbox(c)
	if err != nil {
		return
	}
	if !sb.MCPEnabled {
		c.JSON(409, gin.H{"error": "MCP is not enabled for this sandbox"})
		return
	}
	var request struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if c.ShouldBindJSON(&request) != nil || request.JSONRPC != "2.0" {
		c.JSON(400, gin.H{"jsonrpc": "2.0", "id": nil, "error": gin.H{"code": -32600, "message": "invalid JSON-RPC request"}})
		return
	}
	if len(request.ID) == 0 {
		c.Status(202)
		return
	}
	failure := func(code int, message string) {
		c.JSON(200, gin.H{"jsonrpc": "2.0", "id": request.ID, "error": gin.H{"code": code, "message": message}})
	}
	var result any
	switch request.Method {
	case "initialize":
		result = gin.H{"protocolVersion": "2025-11-25", "capabilities": gin.H{"tools": gin.H{"listChanged": false}}, "serverInfo": gin.H{"name": "sandboxing-go", "version": "0.2.0"}}
	case "ping":
		result = gin.H{}
	case "tools/list":
		schema := func(properties gin.H, required []string) gin.H {
			return gin.H{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
		}
		text := gin.H{"type": "string"}
		result = gin.H{"tools": []any{gin.H{"name": "exec", "description": "Run a command in the authenticated caller's sandbox.", "inputSchema": schema(gin.H{"cmd": text}, []string{"cmd"})}, gin.H{"name": "read_file", "description": "Read a file in the authenticated caller's sandbox.", "inputSchema": schema(gin.H{"path": text}, []string{"path"})}, gin.H{"name": "write_file", "description": "Write a file in the authenticated caller's sandbox.", "inputSchema": schema(gin.H{"path": text, "data": text}, []string{"path", "data"})}}}
	case "tools/call":
		var params struct {
			Name      string            `json:"name"`
			Arguments map[string]string `json:"arguments"`
		}
		if json.Unmarshal(request.Params, &params) != nil {
			failure(-32602, "invalid tool arguments")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		var output string
		var callErr error
		switch params.Name {
		case "exec":
			if len(params.Arguments) != 1 || params.Arguments["cmd"] == "" {
				failure(-32602, "cmd is required")
				return
			}
			v, e := h.k8s.Exec(ctx, sb.PodName, params.Arguments["cmd"])
			callErr = e
			if e == nil {
				b, _ := json.Marshal(v)
				output = string(b)
			}
		case "read_file":
			if len(params.Arguments) != 1 || params.Arguments["path"] == "" {
				failure(-32602, "path is required")
				return
			}
			v, e := h.k8s.ReadFile(ctx, sb.PodName, params.Arguments["path"])
			callErr = e
			output = string(v)
		case "write_file":
			if len(params.Arguments) != 2 || params.Arguments["path"] == "" {
				failure(-32602, "path and data are required")
				return
			}
			callErr = h.k8s.WriteFile(ctx, sb.PodName, params.Arguments["path"], []byte(params.Arguments["data"]))
			output = "File written."
		default:
			failure(-32602, "tool is not allowlisted")
			return
		}
		failed := callErr != nil
		if failed {
			output = "Sandbox tool operation failed."
		}
		result = gin.H{"content": []any{gin.H{"type": "text", "text": output}}, "isError": failed}
		h.auditor.Log(ctx, "mcp.tool_call", "api_key", h.tenantID(c), sb.ID, map[string]any{"tool": params.Name, "success": !failed}, nil)
	default:
		failure(-32601, fmt.Sprintf("unsupported method: %s", request.Method))
		return
	}
	c.JSON(200, gin.H{"jsonrpc": "2.0", "id": request.ID, "result": result})
}
