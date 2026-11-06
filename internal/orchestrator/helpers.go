package orchestrator

import (
	"context"
	"fmt"

	"github.com/yourorg/sandboxing/pkg/models"
)

// ExecInSandbox runs a command inside a sandbox pod.
func (o *Orchestrator) ExecInSandbox(ctx context.Context, sb *models.Sandbox, cmd string) (*models.ExecResult, error) {
	if sb.PodName == "" {
		return nil, fmt.Errorf("sandbox %s has no pod", sb.ID)
	}
	return o.k8s.Exec(ctx, sb.PodName, cmd)
}

// WriteFileInSandbox writes data to a path inside a sandbox pod.
func (o *Orchestrator) WriteFileInSandbox(ctx context.Context, sb *models.Sandbox, path string, data []byte) error {
	if sb.PodName == "" {
		return fmt.Errorf("sandbox %s has no pod", sb.ID)
	}
	return o.k8s.WriteFile(ctx, sb.PodName, path, data)
}

// ReadFileInSandbox reads a file from inside a sandbox pod.
func (o *Orchestrator) ReadFileInSandbox(ctx context.Context, sb *models.Sandbox, path string) ([]byte, error) {
	if sb.PodName == "" {
		return nil, fmt.Errorf("sandbox %s has no pod", sb.ID)
	}
	return o.k8s.ReadFile(ctx, sb.PodName, path)
}
