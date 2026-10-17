package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Auditor writes append-only audit records to PostgreSQL.
// Rows are NEVER updated or deleted — the table only grows.
type Auditor struct {
	store *store.DB
}

func New(store *store.DB) *Auditor {
	return &Auditor{store: store}
}

// Log records an audit entry. Secret values are NEVER logged — only names.
func (a *Auditor) Log(
	ctx context.Context,
	action, actorType, actorID, sandboxID string,
	meta map[string]interface{},
	secretsUsed []string,
) {
	entry := &models.AuditEntry{
		ID:          fmt.Sprintf("aud-%d", time.Now().UnixNano()),
		Action:      action,
		ActorType:   actorType,
		ActorID:     actorID,
		SandboxID:   sandboxID,
		Meta:        meta,
		SecretsUsed: secretsUsed, // names only — values never stored
		CreatedAt:   time.Now(),
	}
	// fire and forget — audit must never block the request path
	go a.store.InsertAuditEntry(ctx, entry)
}
