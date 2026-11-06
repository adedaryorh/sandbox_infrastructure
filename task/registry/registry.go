package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Registry stores versioned task definitions.
// Each deploy creates a new immutable version.
// Runs in progress always finish on the version they started.
type Registry struct {
	store *store.DB
}

func New(store *store.DB) *Registry {
	return &Registry{store: store}
}

// Deploy creates a new version of a task definition.
// If the task name already exists, the version is incremented.
func (r *Registry) Deploy(ctx context.Context, spec *DeploySpec) (*models.TaskDefinition, error) {
	if spec == nil || spec.Name == "" || len(spec.Handler) == 0 {
		return nil, fmt.Errorf("task name and handler are required")
	}
	if spec.RetryMax < 0 || spec.RetryMax > 100 || spec.RetryBackoffMs < 0 || spec.RetryBackoffMs > 300000 || spec.ConcurrencyLimit < 0 || spec.ConcurrencyLimit > 1000 {
		return nil, fmt.Errorf("invalid retry or concurrency limits")
	}
	if spec.Checkpoints && (os.Getenv("ENABLE_CHECKPOINTS") != "true" || spec.DiskGB < 1 || spec.DiskGB > 100) {
		return nil, fmt.Errorf("checkpoint tasks require ENABLE_CHECKPOINTS and disk size 1-100 GiB")
	}
	// get current latest version
	current, err := r.store.GetLatestTaskDefinition(ctx, spec.Name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	newVersion := 1
	if err == nil {
		newVersion = current.Version + 1
	}

	def := &models.TaskDefinition{
		ID:          models.NewID("task"),
		Checkpoints: spec.Checkpoints, DiskGB: spec.DiskGB,
		Name:             spec.Name,
		Version:          newVersion,
		Template:         spec.Template,
		Memory:           spec.Memory,
		CPU:              spec.CPU,
		Isolation:        spec.Isolation,
		Handler:          spec.Handler,
		RetryMax:         spec.RetryMax,
		RetryBackoffMs:   spec.RetryBackoffMs,
		ConcurrencyLimit: spec.ConcurrencyLimit,
		ConcurrencyKey:   spec.ConcurrencyKey,
		QueueName:        spec.QueueName,
		CreatedAt:        time.Now(),
	}

	// apply defaults
	if def.Template == "" {
		def.Template = "python"
	}
	if def.Memory == "" {
		def.Memory = "512mb"
	}
	if def.CPU == "" {
		def.CPU = "0.5"
	}
	if def.Isolation == "" {
		def.Isolation = models.IsolationSecure
	}
	if def.RetryMax == 0 {
		def.RetryMax = 3
	}
	if def.RetryBackoffMs == 0 {
		def.RetryBackoffMs = 1000
	}
	if def.ConcurrencyLimit == 0 {
		def.ConcurrencyLimit = 10
	}
	if def.QueueName == "" {
		def.QueueName = "default"
	}

	if err := r.store.InsertTaskDefinition(ctx, def); err != nil {
		return nil, fmt.Errorf("insert task definition: %w", err)
	}

	return def, nil
}

// Get returns the latest version of a task definition by name.
func (r *Registry) Get(ctx context.Context, name string) (*models.TaskDefinition, error) {
	return r.store.GetLatestTaskDefinition(ctx, name)
}

// DeploySpec is the input to Deploy — what the CLI/SDK sends when deploying a task.
type DeploySpec struct {
	Checkpoints      bool                 `json:"checkpoints"`
	DiskGB           int                  `json:"disk_gb"`
	Name             string               `json:"name"`
	Template         string               `json:"template"`
	Memory           string               `json:"memory"`
	CPU              string               `json:"cpu"`
	Isolation        models.IsolationTier `json:"isolation"`
	Handler          []byte               `json:"handler"` // compiled handler code (Python, Go binary, etc.)
	RetryMax         int                  `json:"retry_max"`
	RetryBackoffMs   int                  `json:"retry_backoff_ms"`
	ConcurrencyLimit int                  `json:"concurrency_limit"`
	ConcurrencyKey   string               `json:"concurrency_key"`
	QueueName        string               `json:"queue_name"`
}
