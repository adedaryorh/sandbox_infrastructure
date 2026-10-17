package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Emitter delivers HMAC-SHA256 signed webhook events to tenant endpoints.
// Each delivery runs in a goroutine with exponential backoff retries.
type Emitter struct {
	store      *store.DB
	httpClient *http.Client
}

func NewEmitter(store *store.DB) *Emitter {
	return &Emitter{
		store: store,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Emit fires an event to all registered endpoints for the tenant.
// Each delivery is async — the caller is never blocked.
func (e *Emitter) Emit(ctx context.Context, tenantID, eventType string, payload interface{}) {
	endpoints, err := e.store.GetWebhookEndpointsForEvent(ctx, tenantID, eventType)
	if err != nil || len(endpoints) == 0 {
		return
	}

	event := &models.WebhookEvent{
		ID:        fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		Type:      eventType,
		Payload:   payload,
		CreatedAt: time.Now(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		logger.Error("marshal webhook event", "err", err)
		return
	}

	for _, ep := range endpoints {
		go e.deliver(ep, body)
	}
}

// deliver sends the event to a single endpoint with up to 3 retries.
func (e *Emitter) deliver(ep *models.WebhookEndpoint, body []byte) {
	sig := e.sign(body, []byte(ep.Secret))

	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		req, err := http.NewRequest("POST", ep.URL, bytes.NewReader(body))
		if err != nil {
			logger.Error("build webhook request", "url", ep.URL, "err", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Sandbox-Signature", "sha256="+sig)
		req.Header.Set("X-Sandbox-Event", "")

		resp, err := e.httpClient.Do(req)
		if err != nil {
			logger.Warn("webhook delivery failed", "url", ep.URL, "attempt", attempt, "err", err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return // success
		}
		logger.Warn("webhook non-2xx", "url", ep.URL, "status", resp.StatusCode)
	}

	logger.Error("webhook delivery exhausted retries", "url", ep.URL)
}

// Customers verify: X-Sandbox-Signature == "sha256=" + HMAC(secret, body)
func (e *Emitter) sign(body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify is the helper customers use in their server to validate incoming webhooks.
func Verify(body []byte, signature string, secret string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}
