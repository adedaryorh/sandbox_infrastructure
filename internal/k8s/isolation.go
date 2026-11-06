package k8s

import (
	"context"
	network "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Client) EnsureIsolation(ctx context.Context) error {
	policy := &network.NetworkPolicy{ObjectMeta: meta.ObjectMeta{Name: "sandbox-baseline", Namespace: c.namespace}, Spec: network.NetworkPolicySpec{PodSelector: meta.LabelSelector{MatchLabels: map[string]string{"app": "sandbox"}}, PolicyTypes: []network.PolicyType{network.PolicyTypeIngress, network.PolicyTypeEgress}, Ingress: []network.NetworkPolicyIngressRule{{From: []network.NetworkPolicyPeer{{PodSelector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "orchestrator"}}}}}}}}
	_, err := c.clientset.NetworkingV1().NetworkPolicies(c.namespace).Create(ctx, policy, meta.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
