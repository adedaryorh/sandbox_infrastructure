package models

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

type tenantKey struct{}

func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}
func Tenant(ctx context.Context) (string, error) {
	v, _ := ctx.Value(tenantKey{}).(string)
	if v == "" {
		return "", errors.New("authenticated tenant context required")
	}
	return v, nil
}
func NewID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("secure random source unavailable")
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

type leaseKey struct{}

func WithLease(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, leaseKey{}, token)
}
func Lease(ctx context.Context) string { v, _ := ctx.Value(leaseKey{}).(string); return v }
