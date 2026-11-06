package orchestrator

import (
	"fmt"
	"github.com/yourorg/sandboxing/pkg/models"
	"k8s.io/apimachinery/pkg/api/resource"
	"regexp"
	"strings"
	"time"
)

func WarmCompatible(r *models.CreateSandboxRequest) bool {
	cpu, e := resource.ParseQuantity(r.CPU)
	if e != nil {
		return false
	}
	mem, e := resource.ParseQuantity(normalizeMemory(r.Memory))
	if e != nil {
		return false
	}
	return r.Isolation == models.IsolationStandard && cpu.Cmp(resource.MustParse("500m")) == 0 && mem.Cmp(resource.MustParse("512Mi")) == 0 && len(r.Env) == 0 && len(r.Secrets) == 0 && !r.Persistent && r.DiskGB == 0 && r.GPU == nil && r.Network == nil && r.MCP == nil && (r.Region == "" || r.Region == "default")
}
func normalizeMemory(s string) string {
	v := strings.ToLower(s)
	return strings.NewReplacer("mb", "Mi", "gb", "Gi", "kb", "Ki", "mi", "Mi", "gi", "Gi", "ki", "Ki").Replace(v)
}
func ValidateRequest(r *models.CreateSandboxRequest) error {
	if strings.TrimSpace(r.Template) == "" || len(r.Template) > 255 {
		return fmt.Errorf("template required")
	}
	if r.Isolation != "standard" && r.Isolation != "secure" {
		return fmt.Errorf("invalid isolation tier")
	}
	cpu, e := resource.ParseQuantity(r.CPU)
	if e != nil || cpu.Sign() <= 0 || cpu.Cmp(resource.MustParse("128")) > 0 {
		return fmt.Errorf("CPU must be positive and at most 128 cores")
	}
	mem, e := resource.ParseQuantity(normalizeMemory(r.Memory))
	if e != nil || mem.Cmp(resource.MustParse("16Mi")) < 0 || mem.Cmp(resource.MustParse("256Gi")) > 0 {
		return fmt.Errorf("memory must be between 16Mi and 256Gi")
	}
	if r.Persistent && (r.DiskGB < 1 || r.DiskGB > 1024) {
		return fmt.Errorf("persistent sandboxes require disk_gb between 1 and 1024")
	}
	if !r.Persistent && r.DiskGB != 0 {
		return fmt.Errorf("disk_gb requires persistent storage")
	}
	if r.TTL != "" {
		d, e := time.ParseDuration(r.TTL)
		if e != nil || d <= 0 || d > 24*time.Hour {
			return fmt.Errorf("TTL must be positive and at most 24h")
		}
	}
	if r.Region != "default" && r.Region != "" && !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`).MatchString(r.Region) {
		return fmt.Errorf("invalid region")
	}
	if r.Network != nil {
		if len(r.Network.AllowDomains) > 0 {
			return fmt.Errorf("domain allowlists require an operator-provided egress proxy; refusing to ignore the request")
		}
		for _, p := range r.Network.Expose {
			if p.Port < 1024 || p.Port > 65535 {
				return fmt.Errorf("preview port must be 1024-65535")
			}
		}
	}
	if r.GPU != nil {
		if r.GPU.Count < 1 || r.GPU.Count > 8 {
			return fmt.Errorf("GPU count must be 1-8")
		}
		if r.GPU.Resource != "" && r.GPU.Resource != "nvidia.com/gpu" && r.GPU.Resource != "amd.com/gpu" {
			return fmt.Errorf("unsupported GPU resource")
		}
		if r.GPU.RuntimeClass != "" && r.Isolation == models.IsolationSecure && r.GPU.RuntimeClass != "gvisor" {
			return fmt.Errorf("GPU runtime cannot override requested secure isolation")
		}
	}
	return nil
}
