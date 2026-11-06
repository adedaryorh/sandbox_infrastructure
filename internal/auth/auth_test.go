package auth

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/pkg/models"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type memoryKeys struct {
	fail bool
	keys map[string]string
}

func (m memoryKeys) LookupAPIKey(ctx context.Context, hash string) (string, error) {
	if m.fail {
		return "", errors.New("database offline")
	}
	v, ok := m.keys[hash]
	if !ok {
		return "", ErrInvalid
	}
	return v, nil
}
func TestAuthenticationDoesNotTrustPrefixesOrQueryKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	a := strings.Repeat("samehead", 4) + "-a"
	b := strings.Repeat("samehead", 4) + "-b"
	for _, tc := range []struct {
		name, header, query, wantTenant string
		fail                            bool
		status                          int
	}{{"missing", "", "", "", false, 401}, {"query rejected", "", "?api_key=" + a, "", false, 401}, {"short", "Bearer short", "", "", false, 401}, {"prefix impostor", "Bearer " + a + "fake", "", "", false, 401}, {"tenant a", "Bearer " + a, "", "tenant-a", false, 200}, {"same prefix tenant b", "Bearer " + b, "", "tenant-b", false, 200}, {"store outage", "Bearer " + a, "", "", true, 503}} {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.Use(Middleware(memoryKeys{fail: tc.fail, keys: map[string]string{Hash(a): "tenant-a", Hash(b): "tenant-b"}}))
			router.GET("/private", func(c *gin.Context) {
				tenant, e := models.Tenant(c.Request.Context())
				if e != nil || tenant != tc.wantTenant {
					t.Errorf("incorrect tenant context: %q %v", tenant, e)
				}
				c.Status(200)
			})
			req := httptest.NewRequest("GET", "/private"+tc.query, nil)
			req.Header.Set("Authorization", tc.header)
			record := httptest.NewRecorder()
			router.ServeHTTP(record, req)
			if record.Code != tc.status {
				t.Fatalf("got %d, want %d", record.Code, tc.status)
			}
		})
	}
}
func TestHealthNeedsNoAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Middleware(memoryKeys{fail: true}))
	r.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
