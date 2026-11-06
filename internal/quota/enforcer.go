package quota

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/provider"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/logger"
)

type Enforcer struct {
	runtime  provider.Runtime
	interval time.Duration
	samples  map[string]time.Time
	store    *store.DB
	webhooks *webhook.Emitter
}

func New(store *store.DB, webhooks *webhook.Emitter) *Enforcer {
	return &Enforcer{store: store, webhooks: webhooks}
}

func (e *Enforcer) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tenantID := c.GetString("tenant_id")
		if tenantID == "" {
			c.Next()
			return
		}
		if c.Request.Method != http.MethodPost || c.FullPath() != "/v1/sandboxes" {
			c.Next()
			return
		}
		if err := e.Check(c.Request.Context(), tenantID); err != nil {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": err.Error(),
				"code":  "quota_exceeded",
			})
			return
		}
		c.Next()
	}
}

func (e *Enforcer) Check(ctx context.Context, tenantID string) error {
	usage, err := e.store.GetQuotaUsage(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("quota database unavailable: %w", err)
	}
	limits, err := e.store.GetQuotaLimits(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("quota database unavailable: %w", err)
	}
	if usage.ConcurrentSandboxes >= limits.ConcurrentSandboxes {
		return fmt.Errorf("concurrent sandbox limit reached (%d/%d)",
			usage.ConcurrentSandboxes, limits.ConcurrentSandboxes)
	}
	if usage.CPUHoursThisMonth >= limits.CPUHoursThisMonth {
		return fmt.Errorf("monthly CPU hour limit reached (%.1f/%.1f hours)",
			usage.CPUHoursThisMonth, limits.CPUHoursThisMonth)
	}
	if usage.CPUHoursThisMonth >= limits.CPUHoursThisMonth*0.8 {
		go e.webhooks.Emit(ctx, tenantID, "quota.warning", map[string]interface{}{
			"cpu_hours_used":  usage.CPUHoursThisMonth,
			"cpu_hours_limit": limits.CPUHoursThisMonth,
		})
	}
	return nil
}

func (e *Enforcer) BillingLoop(ctx context.Context) {
	if e.interval < time.Second {
		e.interval = time.Second
	}
	e.samples = map[string]time.Time{}
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.billOnce(ctx)
		}
	}
}

func (e *Enforcer) Configure(runtime provider.Runtime, seconds int) {
	e.runtime = runtime
	e.interval = time.Duration(seconds) * time.Second
}
func (e *Enforcer) billOnce(ctx context.Context) {
	if e.runtime == nil {
		return
	}
	rows, err := e.store.Raw().QueryContext(ctx, "select id,tenant_id,pod_name from sandboxes where status='running'")
	if err != nil {
		logger.Error("resource accounting database unavailable")
		return
	}
	type workload struct{ id, tenant, pod string }
	items := []workload{}
	for rows.Next() {
		var v workload
		if err = rows.Scan(&v.id, &v.tenant, &v.pod); err != nil {
			rows.Close()
			return
		}
		items = append(items, v)
	}
	rows.Close()
	seen := map[string]bool{}
	for _, v := range items {
		seen[v.id] = true
		sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		metrics, err := e.runtime.GetMetrics(sampleCtx, v.pod)
		cancel()
		if err != nil {
			delete(e.samples, v.id)
			logger.Warn("resource sample unavailable", "sandbox_id", v.id)
			continue
		}
		last := e.samples[v.id]
		e.samples[v.id] = metrics.SampledAt
		if last.IsZero() {
			continue
		}
		elapsed := metrics.SampledAt.Sub(last).Seconds()
		if elapsed <= 0 || elapsed > 4*e.interval.Seconds() || metrics.CPUCoreUsage < 0 {
			continue
		}
		if err = e.store.IncrementCPUSeconds(ctx, v.tenant, metrics.CPUCoreUsage*elapsed); err != nil {
			logger.Error("resource usage persistence failed")
		}
	}
	for id := range e.samples {
		if !seen[id] {
			delete(e.samples, id)
		}
	}
}
