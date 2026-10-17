package quota

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/logger"
)

type Enforcer struct {
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
		return nil
	}
	limits, err := e.store.GetQuotaLimits(ctx, tenantID)
	if err != nil {
		return nil
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
	ticker := time.NewTicker(time.Second)
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

func (e *Enforcer) billOnce(ctx context.Context) {
	rows, err := e.store.Raw().QueryContext(ctx, `
		SELECT tenant_id, COUNT(*) as count
		FROM sandboxes WHERE status='running'
		GROUP BY tenant_id`)
	if err != nil {
		logger.Error("billing query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var tenantID string
		var count float64
		rows.Scan(&tenantID, &count)
		e.store.IncrementCPUSeconds(ctx, tenantID, count)
	}
}
