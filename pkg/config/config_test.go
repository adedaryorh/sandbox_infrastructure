package config

import (
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	cfg := Load()
	if cfg.DefaultCPU != "500m" {
		t.Errorf("expected DefaultCPU 500m, got %s", cfg.DefaultCPU)
	}
	if cfg.DefaultMemory != "512Mi" {
		t.Errorf("expected DefaultMemory 512Mi, got %s", cfg.DefaultMemory)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("DEFAULT_CPU", "1000m")
	t.Setenv("DEFAULT_MEMORY", "1Gi")

	cfg := Load()
	if cfg.DefaultCPU != "1000m" {
		t.Errorf("expected DefaultCPU 1000m, got %s", cfg.DefaultCPU)
	}
	if cfg.DefaultMemory != "1Gi" {
		t.Errorf("expected DefaultMemory 1Gi, got %s", cfg.DefaultMemory)
	}
}
