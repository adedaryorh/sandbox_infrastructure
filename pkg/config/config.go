package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port        string
	DatabaseURL string

	KubeConfigPath  string
	SandboxNS       string
	WarmPoolTargets map[string]int

	CaddyAdminURL     string
	PlatformDomain    string
	WarmPoolRefillSec int

	BillingTickSec int

	WebhookWorkers int
	TaskWorkers    int
}

func Load() *Config {
	return &Config{
		Port:           getEnv("PORT", "8080"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/sandboxing?sslmode=disable"),
		KubeConfigPath: getEnv("KUBECONFIG", ""),
		SandboxNS:      getEnv("SANDBOX_NAMESPACE", "sandboxes"),
		WarmPoolTargets: map[string]int{
			"python": getEnvInt("WARM_PYTHON", 10),
			"node":   getEnvInt("WARM_NODE", 10),
			"ubuntu": getEnvInt("WARM_UBUNTU", 5),
		},
		CaddyAdminURL:     getEnv("CADDY_ADMIN_URL", "http://localhost:2019"),
		PlatformDomain:    getEnv("PLATFORM_DOMAIN", "sandbox.yourplatform.com"),
		WarmPoolRefillSec: getEnvInt("WARM_POOL_REFILL_SEC", 10),
		BillingTickSec:    getEnvInt("BILLING_TICK_SEC", 1),
		WebhookWorkers:    getEnvInt("WEBHOOK_WORKERS", 5),
		TaskWorkers:       getEnvInt("TASK_WORKERS", 10),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
