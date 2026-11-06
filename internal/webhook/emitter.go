package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

type Emitter struct {
	store        *store.DB
	httpClient   *http.Client
	allowPrivate bool
}

func NewEmitter(db *store.DB) *Emitter {
	allow := os.Getenv("WEBHOOK_ALLOW_PRIVATE") == "true"
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
		if e != nil {
			return nil, e
		}
		for _, ip := range ips {
			if !allow && !publicIP(ip.IP) {
				return nil, fmt.Errorf("webhook destination is not public")
			}
		}
		for _, ip := range ips {
			connection, e := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if e == nil {
				return connection, nil
			}
		}
		return nil, fmt.Errorf("webhook destination unreachable")
	}}
	return &Emitter{store: db, allowPrivate: allow, httpClient: &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func publicIP(ip net.IP) bool {
	// Shared, reserved and translation ranges are not public webhook destinations.
	for _, block := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "64:ff9b::/96", "64:ff9b:1::/48", "2001:db8::/32", "2002::/16"} {
		_, network, _ := net.ParseCIDR(block)
		if network.Contains(ip) {
			return false
		}
	}

	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsUnspecified()
}
func (e *Emitter) ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("invalid webhook URL")
	}
	if u.Scheme != "https" && !(e.allowPrivate && u.Scheme == "http") {
		return fmt.Errorf("webhooks require HTTPS")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !e.allowPrivate && !publicIP(ip) {
		return fmt.Errorf("private webhook destinations are disabled")
	}
	return nil
}
func (e *Emitter) Emit(ctx context.Context, tenant, eventType string, payload any) {
	if scoped, err := models.Tenant(ctx); err == nil && scoped != tenant {
		return
	}
	endpoints, err := e.store.GetWebhookEndpointsForEvent(ctx, tenant, eventType)
	if err != nil {
		logger.Error("load webhook endpoints failed")
		return
	}
	event := models.WebhookEvent{ID: models.NewID("event"), Type: eventType, Payload: payload, CreatedAt: time.Now().UTC()}
	body, err := json.Marshal(event)
	if err != nil {
		logger.Error("encode webhook event failed")
		return
	}
	tx, err := e.store.Raw().BeginTx(ctx, nil)
	if err != nil {
		logger.Error("begin webhook outbox transaction failed")
		return
	}
	defer tx.Rollback()
	for _, endpoint := range endpoints {
		if _, err = tx.ExecContext(ctx, `insert into webhook_outbox(id,endpoint_id,tenant_id,event_type,payload) values($1,$2,$3,$4,$5)`, models.NewID("delivery"), endpoint.ID, tenant, eventType, string(body)); err != nil {
			logger.Error("persist webhook delivery failed")
			return
		}
	}
	if err = tx.Commit(); err != nil {
		logger.Error("commit webhook deliveries failed")
	}
}
func (e *Emitter) Start(ctx context.Context, workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					e.once(ctx)
				}
			}
		}()
	}
	<-ctx.Done()
}
func (e *Emitter) once(ctx context.Context) {
	e.store.Raw().ExecContext(ctx, `update webhook_outbox set status='dead',last_error='delivery lease expired after final attempt' where status='running' and locked_until<now() and attempts>=5`)
	token := models.NewID("delivery-lease")
	var id, endpoint, event string
	var body []byte
	var attempt int
	err := e.store.Raw().QueryRowContext(ctx, `update webhook_outbox set status='running',attempts=attempts+1,locked_until=now()+interval '30 seconds',lease_token=$1
 where id=(select id from webhook_outbox where attempts<5 and available_at<=now() and (status='queued' or(status='running' and locked_until<now())) order by created_at limit 1 for update skip locked)
 returning id,endpoint_id,event_type,payload,attempts`, token).Scan(&id, &endpoint, &event, &body, &attempt)
	if err != nil {
		return
	}
	var destination, secret string
	err = e.store.Raw().QueryRowContext(ctx, "select url,secret from webhook_endpoints where id=$1", endpoint).Scan(&destination, &secret)
	delivered := false
	if err == nil && e.ValidateURL(destination) == nil {
		req, buildErr := http.NewRequestWithContext(ctx, "POST", destination, bytes.NewReader(body))
		if buildErr == nil {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Sandbox-Signature", e.sign(body, []byte(secret)))
			req.Header.Set("X-Sandbox-Event", event)
			req.Header.Set("X-Sandbox-Delivery-ID", id)
			response, sendErr := e.httpClient.Do(req)
			if sendErr == nil {
				delivered = response.StatusCode >= 200 && response.StatusCode < 300
				response.Body.Close()
			}
		}
	}
	status := "queued"
	if delivered {
		status = "delivered"
	} else if attempt >= 5 {
		status = "dead"
	}
	delay := time.Now().Add(Backoff(attempt))
	message := ""
	if !delivered {
		message = "delivery failed; inspect receiver availability and destination policy"
	}
	if _, err = e.store.Raw().ExecContext(ctx, `update webhook_outbox set status=$1,available_at=$2,locked_until=null,last_error=$3 where id=$4 and lease_token=$5 and status='running'`, status, delay, message, id, token); err != nil {
		logger.Error("persist webhook delivery outcome failed")
	}
}
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Second * time.Duration(1<<uint(attempt-1))
}
func (e *Emitter) sign(body, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
func Verify(body []byte, signature, secret string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal([]byte("sha256="+hex.EncodeToString(mac.Sum(nil))), []byte(signature))
}

// Preserve explicit SHA-256 prefix in the wire signature; never log receiver secrets or payloads.
