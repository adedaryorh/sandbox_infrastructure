package provider

import (
	"context"

	"github.com/yourorg/sandboxing/pkg/models"
)

type Provider interface {
	CreatePod(ctx context.Context, req *models.CreateSandboxRequest, sandboxID string) (string, error)
	DeletePod(ctx context.Context, podName string) error
	WaitForPodRunning(ctx context.Context, podName string) error
	GetPodIP(ctx context.Context, podName string) (string, error)
	CreatePVC(ctx context.Context, sandboxID string, diskGB int) error
	DeletePVC(ctx context.Context, sandboxID string) error
	CreateSecret(ctx context.Context, name string, data map[string]string) error
	DeleteSecret(ctx context.Context, name string) error
	Exec(ctx context.Context, podName string, cmd string) (*models.ExecResult, error)
	WriteFile(ctx context.Context, podName string, path string, data []byte) error
	ReadFile(ctx context.Context, podName string, path string) ([]byte, error)
	DeleteFile(ctx context.Context, podName string, path string) error
	ListDir(ctx context.Context, podName string, path string) (string, error)
}
