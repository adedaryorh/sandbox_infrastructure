package api

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
	"net/http"
	"net/url"
	"strings"
)

func (h *Handler) Ownership(c *gin.Context) {
	if id := c.Param("id"); id != "" && strings.HasPrefix(c.FullPath(), "/v1/sandboxes/") {
		sb, err := h.store.GetSandbox(c.Request.Context(), id)
		if err != nil {
			apiError(c, err)
			return
		}
		if h.LocalRegion != "" && sb.Region != h.LocalRegion {
			c.AbortWithStatusJSON(409, gin.H{"error": "use the sandbox control plane in its recorded region", "region": sb.Region})
			return
		}
		c.Set("owned_sandbox", sb)
		if rid := c.Param("rid"); rid != "" && !h.replManager.BelongsTo(rid, id) {
			c.AbortWithStatusJSON(404, gin.H{"error": "session not found"})
			return
		}
	}
	if strings.HasPrefix(c.FullPath(), "/v1/checkpoints/") {
		if _, err := h.store.GetCheckpoint(c.Request.Context(), c.Param("id")); err != nil {
			apiError(c, err)
			return
		}
	}
	if c.Request.ContentLength > 2<<20 {
		c.AbortWithStatusJSON(413, gin.H{"error": "request exceeds 2 MiB"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2<<20)
	c.Next()
}
func apiError(c *gin.Context, err error) {
	if errors.Is(err, store.ErrNotFound) {
		c.AbortWithStatusJSON(404, gin.H{"error": "resource not found"})
		return
	}
	if errors.Is(err, store.ErrBadTransition) {
		c.AbortWithStatusJSON(409, gin.H{"error": "resource state changed"})
		return
	}
	if errors.Is(err, store.ErrQuotaExceeded) {
		c.AbortWithStatusJSON(429, gin.H{"error": "quota exceeded"})
		return
	}
	c.AbortWithStatusJSON(500, gin.H{"error": "operation failed"})
}
func tenantContext(c *gin.Context) (context.Context, string) {
	t := c.GetString("tenant_id")
	return models.WithTenant(c.Request.Context(), t), t
}
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host) && (u.Scheme == "http" || u.Scheme == "https")
}
