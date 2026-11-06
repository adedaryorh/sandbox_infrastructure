package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Backend         string
	BootstrapKey    string
	BootstrapTenant string
	PublicAPIURL    string
	Checkpoints     bool

	Port        string
	DatabaseURL string

	KubeConfigPath  string
	SandboxNS       string
	DefaultCPU      string
	DefaultMemory   string
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
		Backend:         getEnv("SANDBOX_BACKEND", "kubernetes"),
		BootstrapKey:    os.Getenv("SANDBOX_BOOTSTRAP_API_KEY"),
		BootstrapTenant: getEnv("SANDBOX_BOOTSTRAP_TENANT", "local"),
		PublicAPIURL:    getEnv("PUBLIC_API_URL", "http://localhost:8080"),
		Checkpoints:     getEnv("ENABLE_CHECKPOINTS", "false") == "true",

		Port:           getEnv("PORT", "8080"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/sandboxing?sslmode=disable"),
		KubeConfigPath: getEnv("KUBECONFIG", ""),
		SandboxNS:      getEnv("SANDBOX_NAMESPACE", "sandboxes"),
		DefaultCPU:     getEnv("DEFAULT_CPU", "500m"),
		DefaultMemory:  getEnv("DEFAULT_MEMORY", "512Mi"),
		WarmPoolTargets: map[string]int{
			"python": getEnvInt("WARM_PYTHON", 0),
			"node":   getEnvInt("WARM_NODE", 0),
			"ubuntu": getEnvInt("WARM_UBUNTU", 0),
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

func (c *Config) Validate() error {
	if c.Backend != "kubernetes" && c.Backend != "docker" {
		return fmt.Errorf("SANDBOX_BACKEND must be kubernetes or docker")
	}
	if c.TaskWorkers < 1 || c.TaskWorkers > 128 || c.WebhookWorkers < 1 || c.WebhookWorkers > 64 || c.BillingTickSec < 1 || c.WarmPoolRefillSec < 1 {
		return fmt.Errorf("invalid worker count or interval")
	}
	for _, n := range c.WarmPoolTargets {
		if n < 0 || n > 100 {
			return fmt.Errorf("warm pool targets must be 0-100")
		}
	}
	return nil
}
