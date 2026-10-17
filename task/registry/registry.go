package registry

import (
	"context"
	"fmt"
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
	// get current latest version
	current, err := r.store.GetLatestTaskDefinition(ctx, spec.Name)
	newVersion := 1
	if err == nil {
		newVersion = current.Version + 1
	}

	def := &models.TaskDefinition{
		ID:               fmt.Sprintf("task-%d", time.Now().UnixNano()),
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
	Name             string
	Template         string
	Memory           string
	CPU              string
	Isolation        models.IsolationTier
	Handler          []byte // compiled handler code (Python, Go binary, etc.)
	RetryMax         int
	RetryBackoffMs   int
	ConcurrencyLimit int
	ConcurrencyKey   string
	QueueName        string
}
