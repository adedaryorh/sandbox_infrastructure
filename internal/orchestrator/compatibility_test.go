package orchestrator

import (
	"github.com/yourorg/sandboxing/pkg/models"
	"testing"
)

func TestWarmPoolCannotDowngradeRequestedConfiguration(t *testing.T) {
	for _, name := range []string{"default", "secure", "cpu", "memory", "env", "secret", "persistent", "gpu", "ports", "region", "mcp"} {
		t.Run(name, func(t *testing.T) {
			r := &models.CreateSandboxRequest{Template: "python", CPU: "500m", Memory: "512Mi", Isolation: models.IsolationStandard}
			switch name {
			case "secure":
				r.Isolation = models.IsolationSecure
			case "cpu":
				r.CPU = "1"
			case "memory":
				r.Memory = "1Gi"
			case "env":
				r.Env = map[string]string{"A": "B"}
			case "secret":
				r.Secrets = []string{"key"}
			case "persistent":
				r.Persistent = true
				r.DiskGB = 1
			case "gpu":
				r.GPU = &models.GPUConfig{Count: 1}
			case "ports":
				r.Network = &models.NetworkConfig{Expose: []models.PortExpose{{Port: 8000}}}
			case "region":
				r.Region = "other"
			case "mcp":
				r.MCP = &models.MCPConfig{Enabled: true}
			}
			want := name == "default"
			if WarmCompatible(r) != want {
				t.Fatalf("incorrect compatibility for %s", name)
			}
		})
	}
}
func TestInvalidSandboxResourcesAreRejected(t *testing.T) {
	for _, value := range []string{"garbage", "-1", "0", "999"} {
		r := &models.CreateSandboxRequest{Template: "python", CPU: value, Memory: "512Mi", Isolation: models.IsolationStandard}
		if ValidateRequest(r) == nil {
			t.Errorf("accepted invalid CPU %s", value)
		}
	}
}
