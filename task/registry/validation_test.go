package registry

import (
	"context"
	"testing"
)

func TestInvalidDeploymentRejectedBeforeDatabase(t *testing.T) {
	registry := New(nil)
	for _, spec := range []*DeploySpec{nil, {}, {Name: "test", Handler: []byte("print(1)"), RetryMax: -1}, {Name: "test", Handler: []byte("print(1)"), ConcurrencyLimit: -1}, {Name: "test", Handler: []byte("print(1)"), RetryBackoffMs: 300001}} {
		if _, err := registry.Deploy(context.Background(), spec); err == nil {
			t.Fatal("invalid deployment accepted")
		}
	}
}
