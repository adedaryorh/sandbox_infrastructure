package k8s

import (
	"context"
	"fmt"
	"github.com/yourorg/sandboxing/pkg/models"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"time"
)

func (c *Client) GetMetrics(ctx context.Context, pod string) (*models.ResourceMetrics, error) {
	value, err := c.dynamic.Resource(schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}).Namespace(c.namespace).Get(ctx, pod, meta.GetOptions{})
	if err != nil {
		return nil, err
	}
	items, ok := value.Object["containers"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid metrics response")
	}
	m := &models.ResourceMetrics{SampledAt: time.Now().UTC(), Source: "kubernetes-metrics-server"}
	for _, item := range items {
		v, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid container metrics")
		}
		u, ok := v["usage"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("missing resource usage")
		}
		cpu, e := resource.ParseQuantity(fmt.Sprint(u["cpu"]))
		if e != nil {
			return nil, e
		}
		mem, e := resource.ParseQuantity(fmt.Sprint(u["memory"]))
		if e != nil {
			return nil, e
		}
		m.CPUCoreUsage += cpu.AsApproximateFloat64()
		m.MemoryBytes += mem.Value()
	}
	return m, nil
}
