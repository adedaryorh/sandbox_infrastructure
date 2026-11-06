package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/yourorg/sandboxing/internal/api"
	"github.com/yourorg/sandboxing/internal/audit"
	"github.com/yourorg/sandboxing/internal/auth"
	"github.com/yourorg/sandboxing/internal/devmode"
	"github.com/yourorg/sandboxing/internal/k8s"
	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/internal/provider"
	"github.com/yourorg/sandboxing/internal/quota"
	"github.com/yourorg/sandboxing/internal/repl"
	preview "github.com/yourorg/sandboxing/internal/router"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/config"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/tracing"
	"github.com/yourorg/sandboxing/task/queue"
	"github.com/yourorg/sandboxing/task/registry"
	"github.com/yourorg/sandboxing/task/runner"
	"github.com/yourorg/sandboxing/task/scheduler"
)

func main() {
	logger.Init()
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
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

	if cfg.BootstrapKey != "" {
		if err := db.ProvisionAPIKey(ctx, cfg.BootstrapTenant, cfg.BootstrapKey); err != nil {
			logger.Error("bootstrap API key failed", "err", err)
			os.Exit(1)
		}
	}
	shutdownTracing, err := tracing.Init(ctx, "sandboxing-go", "0.2.0")
	if err != nil {
		logger.Error("tracing initialization failed", "err", err)
		os.Exit(1)
	}
	defer shutdownTracing()
	// ── Kubernetes ────────────────────────────────────────────────────────────
	var backend provider.Runtime
	if cfg.Backend == "docker" {
		backend, err = devmode.NewRuntime(os.Getenv("DOCKER_WORKSPACE_ROOT"))
	} else {
		backend, err = k8s.New(cfg.KubeConfigPath, cfg.SandboxNS, cfg.DefaultCPU, cfg.DefaultMemory)
	}
	if err != nil {
		logger.Error("runtime initialization failed", "err", err)
		os.Exit(1)
	}
	// ── Core services ─────────────────────────────────────────────────────────
	webhookEmitter := webhook.NewEmitter(db)
	auditor := audit.New(db)
	warmPool := orchestrator.NewWarmPool(db, backend, cfg.WarmPoolTargets, cfg.WarmPoolRefillSec)
	orch := orchestrator.New(db, backend, webhookEmitter, auditor, warmPool)
	if cfg.CaddyAdminURL != "" {
		routing := preview.New(cfg.CaddyAdminURL, cfg.PlatformDomain)
		upstream := os.Getenv("PREVIEW_API_UPSTREAM")
		if upstream != "" {
			if err := routing.SetAPIUpstream(upstream); err != nil {
				logger.Error("invalid preview API upstream")
				os.Exit(1)
			}
		}
		scheme := os.Getenv("PREVIEW_SCHEME")
		if scheme == "" {
			scheme = "http"
		}
		if err := routing.SetScheme(scheme); err != nil {
			logger.Error("invalid preview scheme")
			os.Exit(1)
		}
		orch.SetRouter(routing)
	}
	orch.CheckpointsEnabled = cfg.Checkpoints
	localRegion := os.Getenv("LOCAL_REGION")
	if localRegion == "" {
		localRegion = "default"
	}
	orch.LocalRegion = localRegion
	replManager := repl.NewManager(backend)
	replManager.SetContext(ctx)
	quotaEnforcer := quota.New(db, webhookEmitter)
	quotaEnforcer.Configure(backend, cfg.BillingTickSec)

	// ── Task layer ────────────────────────────────────────────────────────────
	taskRegistry := registry.New(db)
	taskQueue := queue.New(db)
	taskRunner := runner.New(db, orch, cfg.TaskWorkers)
	taskScheduler := scheduler.New(db, taskQueue)

	// ── Background goroutines ─────────────────────────────────────────────────
	// warm pool — keeps pre-started pods ready
	go warmPool.RefillLoop(ctx)
	go webhookEmitter.Start(ctx, cfg.WebhookWorkers)
	// billing — increments CPU usage per second for running sandboxes
	go quotaEnforcer.BillingLoop(ctx)
	// TTL expiry — destroys sandboxes past their TTL
	go orch.ExpireTTLSandboxes(ctx)
	go orch.ReconcileRoutes(ctx)
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
	router.Use(tracing.HTTPMiddleware())
	router.Use(auth.Middleware(db))

	var regions []orchestrator.Region
	if raw := os.Getenv("REGION_ENDPOINTS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &regions); err != nil {
			logger.Error("invalid REGION_ENDPOINTS")
			os.Exit(1)
		}
	}
	regionScheduler := orchestrator.NewRegionScheduler(regions)
	if len(regions) > 0 {
		go regionScheduler.ProbeLoop(ctx)
	}
	regional, e := api.RegionalCreation(localRegion, regions, regionScheduler)
	if e != nil {
		logger.Error("invalid regional configuration", "err", e)
		os.Exit(1)
	}
	router.Use(regional)                   // validates API key, sets tenant_id
	router.Use(quotaEnforcer.Middleware()) // enforces quota before create

	handler := api.NewHandler(db, orch, backend, replManager, webhookEmitter, auditor)
	handler.LocalRegion = localRegion
	handler.SetPublicURL(cfg.PublicAPIURL)
	handler.RegisterRoutes(router)

	// task layer routes — /v1/tasks, /v1/runs, /v1/queues, /v1/schedules
	taskHandler := api.NewTaskHandler(db, taskRegistry, taskQueue, taskScheduler)
	taskHandler.SetRunner(taskRunner)
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
