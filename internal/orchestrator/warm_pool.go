package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/yourorg/sandboxing/internal/k8s"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
)

// WarmPool keeps a set of pre-started pods ready so create() resolves
// in under 100ms instead of waiting for Kubernetes pod scheduling (50-400ms).
type WarmPool struct {
	store     *store.DB
	k8s       *k8s.Client
	targets   map[string]int // template → desired ready count
	refillSec int
}

func NewWarmPool(store *store.DB, k8s *k8s.Client, targets map[string]int, refillSec int) *WarmPool {
	return &WarmPool{
		store:     store,
		k8s:       k8s,
		targets:   targets,
		refillSec: refillSec,
	}
}

// Claim atomically claims a ready warm pod from the pool.
// Uses SELECT FOR UPDATE SKIP LOCKED so concurrent requests never grab the same pod.
// Returns nil, nil when the pool is empty — caller must cold-start.
func (w *WarmPool) Claim(ctx context.Context, template string) (*store.WarmPod, error) {
	return w.store.ClaimWarmPod(ctx, template)
}

// RefillLoop runs forever in a background goroutine.
// Every `refillSec` seconds it checks pool levels and spawns pods to fill gaps.
func (w *WarmPool) RefillLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.refillSec) * time.Second)
	defer ticker.Stop()

	logger.Info("warm pool refill loop started", "targets", w.targets)

	for {
		select {
		case <-ctx.Done():
			logger.Info("warm pool refill loop stopped")
			return
		case <-ticker.C:
			w.refillOnce(ctx)
		}
	}
}

func (w *WarmPool) refillOnce(ctx context.Context) {
	for template, target := range w.targets {
		ready, err := w.store.CountReadyWarmPods(ctx, template)
		if err != nil {
			logger.Error("count warm pods", "template", template, "err", err)
			continue
		}

		deficit := target - ready
		if deficit <= 0 {
			continue
		}

		logger.Info("warm pool refill", "template", template, "ready", ready, "target", target, "spawning", deficit)

		for i := 0; i < deficit; i++ {
			go w.spawnWarmPod(ctx, template)
		}
	}
}

// spawnWarmPod creates one pre-warmed pod and adds it to the pool table.
func (w *WarmPool) spawnWarmPod(ctx context.Context, template string) {
	warmID := fmt.Sprintf("warm-%d", time.Now().UnixNano())

	req := &models.CreateSandboxRequest{
		Template:  template,
		Memory:    "512mb",
		CPU:       "0.5",
		Isolation: models.IsolationStandard, // warm pods use standard tier
		// they'll be upgraded to secure at claim time if needed
	}

	podName, err := w.k8s.CreatePod(ctx, req, warmID)
	if err != nil {
		logger.Error("warm pod create failed", "template", template, "err", err)
		return
	}

	// wait for pod to be fully Running before adding to pool
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := w.k8s.WaitForPodRunning(waitCtx, podName); err != nil {
		logger.Error("warm pod not ready", "pod", podName, "err", err)
		w.k8s.DeletePod(ctx, podName)
		return
	}

	// register in pool table as 'ready' — now claimable
	if err := w.store.InsertWarmPod(ctx, warmID, template, podName, string(models.IsolationStandard)); err != nil {
		logger.Error("insert warm pod", "pod", podName, "err", err)
		w.k8s.DeletePod(ctx, podName)
		return
	}

	logger.Info("warm pod ready", "template", template, "pod", podName)
}

// DrainPool deletes all ready warm pods — called on shutdown.
func (w *WarmPool) DrainPool(ctx context.Context) {
	rows, err := w.store.Raw().QueryContext(ctx,
		`SELECT id, pod_name FROM warm_pool WHERE status='ready'`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, podName string
		rows.Scan(&id, &podName)
		w.k8s.DeletePod(ctx, podName)
		w.store.DeleteWarmPod(ctx, id)
	}
}
