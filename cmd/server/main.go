package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/yourorg/sandboxing/internal/api"
	"github.com/yourorg/sandboxing/internal/audit"
	"github.com/yourorg/sandboxing/internal/k8s"
	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/internal/quota"
	"github.com/yourorg/sandboxing/internal/repl"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/config"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/task/queue"
	"github.com/yourorg/sandboxing/task/registry"
	"github.com/yourorg/sandboxing/task/runner"
	"github.com/yourorg/sandboxing/task/scheduler"
)

func main() {
	logger.Init()
	cfg := config.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// graceful shutdown on SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	// ── Database ──────────────────────────────────────────────────────────────
	db, err := store.New(cfg.DatabaseURL)
	if err != nil {
		logger.Error("connect database", "err", err)
		os.Exit(1)
	}
	logger.Info("database connected")

	// run migrations — creates all tables if they don't exist
	if err := db.Migrate(ctx); err != nil {
		logger.Error("migrate database", "err", err)
		os.Exit(1)
	}
	logger.Info("migrations complete")

	// ── Kubernetes ────────────────────────────────────────────────────────────
	k8sClient, err := k8s.New(cfg.KubeConfigPath, cfg.SandboxNS)
	if err != nil {
		logger.Error("connect kubernetes", "err", err)
		os.Exit(1)
	}
	logger.Info("kubernetes connected", "namespace", cfg.SandboxNS)

	// ── Core services ─────────────────────────────────────────────────────────
	webhookEmitter := webhook.NewEmitter(db)
	auditor := audit.New(db)
	warmPool := orchestrator.NewWarmPool(db, k8sClient, cfg.WarmPoolTargets, cfg.WarmPoolRefillSec)
	orch := orchestrator.New(db, k8sClient, webhookEmitter, auditor, warmPool)
	replManager := repl.NewManager(k8sClient)
	quotaEnforcer := quota.New(db, webhookEmitter)

	// ── Task layer ────────────────────────────────────────────────────────────
	taskRegistry := registry.New(db)
	taskQueue := queue.New(db)
	taskRunner := runner.New(db, orch, cfg.TaskWorkers)
	taskScheduler := scheduler.New(db, taskQueue)

	// ── Background goroutines ─────────────────────────────────────────────────
	// warm pool — keeps pre-started pods ready
	go warmPool.RefillLoop(ctx)
	// billing — increments CPU usage per second for running sandboxes
	go quotaEnforcer.BillingLoop(ctx)
	// TTL expiry — destroys sandboxes past their TTL
	go orch.ExpireTTLSandboxes(ctx)
	// TTL warning — fires webhook 5 minutes before expiry
	go orch.WarnTTLSandboxes(ctx)
	// task runner — worker pool dequeuing and executing task runs
	go taskRunner.Start(ctx)
	// cron scheduler — fires scheduled task runs
	go taskScheduler.Run(ctx)

	logger.Info("all background services started")

	// ── HTTP server ───────────────────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestLogger())
	router.Use(authMiddleware())           // validates API key, sets tenant_id
	router.Use(quotaEnforcer.Middleware()) // enforces quota before create

	handler := api.NewHandler(db, orch, k8sClient, replManager, webhookEmitter, auditor)
	handler.RegisterRoutes(router)

	// task layer routes — /v1/tasks, /v1/runs, /v1/queues, /v1/schedules
	taskHandler := api.NewTaskHandler(db, taskRegistry, taskQueue, taskScheduler)
	taskHandler.RegisterTaskRoutes(router)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // 0 = no timeout — needed for SSE streams
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("server starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			cancel()
		}
	}()

	// ── Wait for shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// drain warm pool — clean up idle pods
	warmPool.DrainPool(shutdownCtx)

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown", "err", err)
	}

	logger.Info("server stopped cleanly")
}

// ── Middleware ────────────────────────────────────────────────────────────────

// authMiddleware validates the Bearer token and sets tenant_id on the context.
// In production: validate against your auth service or JWT.
func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// skip auth for health endpoint
		if c.FullPath() == "/health" {
			c.Next()
			return
		}

		token := c.GetHeader("Authorization")
		if token == "" {
			token = c.Query("api_key")
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing API key — provide Authorization: Bearer <key> header",
			})
			return
		}

		// TODO: look up token in database, get tenant_id
		// For now: use the token itself as the tenant_id (dev mode only)
		tenantID := resolveTenantFromToken(token)
		c.Set("tenant_id", tenantID)
		c.Next()
	}
}

// resolveTenantFromToken looks up the tenant for an API key.
// Replace this with a real database lookup in production.
func resolveTenantFromToken(token string) string {
	// strip "Bearer " prefix if present
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}
	// in production: SELECT tenant_id FROM api_keys WHERE key_hash=SHA256(token)
	return fmt.Sprintf("tenant-%s", token[:min(8, len(token))])
}

// requestLogger logs method, path and latency for every request.
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"tenant", c.GetString("tenant_id"),
		)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
