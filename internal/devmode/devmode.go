package devmode

// DevMode allows developers to run tasks locally using Docker
// while the cloud handles queueing and scheduling.
//
// What dev mode does:
//   - Builds task code using the same pipeline as production
//   - Runs sandboxes locally via Docker (standard tier only — no gVisor locally)
//   - Receives triggers from the cloud queue
//   - Streams logs and traces to your terminal in real-time
//   - Supports breakpoints and debuggers
//
// What dev mode does NOT do:
//   - Use gVisor isolation (Docker only locally)
//   - Support GPU workloads
//   - Produce billable usage
//
// Usage:
//   SANDBOX_DEV_MODE=true go run ./cmd/server
//   # or via CLI:
//   sandbox dev
//
// The DEV_MODE env var switches the k8s client to use a Docker client instead.
// All other code (queue, retry, tracing) runs identically to production.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
)

// IsEnabled returns true when SANDBOX_DEV_MODE=true.
func IsEnabled() bool {
	return os.Getenv("SANDBOX_DEV_MODE") == "true"
}

// DockerRunner runs sandboxes locally using the Docker CLI.
// It mimics the k8s.Client interface so the orchestrator works unchanged.
type DockerRunner struct{}

func New() *DockerRunner {
	return &DockerRunner{}
}

// RunContainer starts a Docker container and returns its ID.
// Equivalent to k8s.Client.CreatePod() but uses Docker locally.
func (d *DockerRunner) RunContainer(ctx context.Context, req *models.CreateSandboxRequest, sandboxID string) (string, error) {
	image := templateToDockerImage(req.Template)
	mem := req.Memory
	if mem == "" {
		mem = "512m"
	}

	args := []string{
		"run", "-d",
		"--name", "sb-" + sandboxID,
		"--memory", mem,
		"--cpus", req.CPU,
		"--rm",
		// workspace volume
		"-v", fmt.Sprintf("sandbox-workspace-%s:/workspace", sandboxID),
		// network isolation
		"--network", "sandbox-net",
		image,
		"sleep", "infinity",
	}

	// inject env vars
	for k, v := range req.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return "", fmt.Errorf("docker run: %w", err)
	}

	containerID := strings.TrimSpace(string(out))
	logger.Info("[dev mode] container started", "sandbox_id", sandboxID, "container", containerID[:12])
	return containerID, nil
}

// Exec runs a command inside a local Docker container.
func (d *DockerRunner) Exec(ctx context.Context, containerID, cmd string) (string, string, int, error) {
	out, err := exec.CommandContext(ctx, "docker", "exec", containerID,
		"/bin/sh", "-c", cmd).CombinedOutput()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	return string(out), "", exitCode, nil
}

// StopContainer removes a local Docker container.
func (d *DockerRunner) StopContainer(ctx context.Context, containerID string) error {
	exec.CommandContext(ctx, "docker", "rm", "-f", containerID).Run()
	return nil
}

// EnsureNetwork creates the sandbox Docker network if it doesn't exist.
func EnsureNetwork(ctx context.Context) {
	exec.CommandContext(ctx, "docker", "network", "create",
		"--driver", "bridge",
		"--internal", // no external internet by default
		"sandbox-net",
	).Run() // ignore error if network already exists
}

func templateToDockerImage(template string) string {
	images := map[string]string{
		"python": "python:3.11-slim",
		"node":   "node:20-slim",
		"ubuntu": "ubuntu:22.04",
		"ruby":   "ruby:3.3-slim",
		"go":     "golang:1.22-bookworm",
	}
	if img, ok := images[template]; ok {
		return img
	}
	return template
}
