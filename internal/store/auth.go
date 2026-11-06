package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/yourorg/sandboxing/internal/auth"
	"regexp"
)

func (s *DB) LookupAPIKey(ctx context.Context, hash string) (string, error) {
	var tenant string
	err := s.db.QueryRowContext(ctx, "select tenant_id from api_keys where key_hash=$1 and revoked=false", hash).Scan(&tenant)
	if err == sql.ErrNoRows {
		return "", auth.ErrInvalid
	}
	return tenant, err
}
func (s *DB) ProvisionAPIKey(ctx context.Context, tenant, key string) error {
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$`).MatchString(tenant) || len(key) < 32 || len(key) > 512 {
		return fmt.Errorf("tenant must be 2-64 safe characters and key at least 32 characters")
	}
	result, err := s.db.ExecContext(ctx, `insert into api_keys(key_hash,tenant_id) values($1,$2) on conflict(key_hash) do update set revoked=false where api_keys.tenant_id=excluded.tenant_id`, auth.Hash(key), tenant)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("key is already assigned to another tenant")
	}
	return nil
}
