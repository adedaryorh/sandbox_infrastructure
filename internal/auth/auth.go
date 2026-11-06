package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/pkg/models"
	"net/http"
	"strings"
)

var ErrInvalid = errors.New("invalid API key")

type KeyStore interface {
	LookupAPIKey(context.Context, string) (string, error)
}

func Hash(key string) string { h := sha256.Sum256([]byte(key)); return hex.EncodeToString(h[:]) }
func Middleware(keys KeyStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.FullPath() == "/health" {
			c.Next()
			return
		}
		fields := strings.Fields(c.GetHeader("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || len(fields[1]) < 32 || len(fields[1]) > 512 {
			c.AbortWithStatusJSON(401, gin.H{"error": "a valid Bearer API key is required"})
			return
		}
		tenant, err := keys.LookupAPIKey(c.Request.Context(), Hash(fields[1]))
		if err != nil {
			code := http.StatusServiceUnavailable
			if errors.Is(err, ErrInvalid) {
				code = 401
			}
			c.AbortWithStatusJSON(code, gin.H{"error": "authentication failed"})
			return
		}
		if c.Request.ContentLength > 2<<20 {
			c.AbortWithStatusJSON(413, gin.H{"error": "request exceeds 2 MiB"})
			return
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2<<20)
		}
		c.Set("tenant_id", tenant)
		c.Request = c.Request.WithContext(models.WithTenant(c.Request.Context(), tenant))
		c.Next()
	}
}
