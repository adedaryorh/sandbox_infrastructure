package orchestrator

import (
	"context"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/yourorg/sandboxing/pkg/logger"
)

// Region represents a geographic deployment of the platform.
// Each region has its own Kubernetes cluster and orchestrator instance.
type Region struct {
	Name     string
	Endpoint string // orchestrator API URL in this region
	Location string // human-readable location e.g. "Frankfurt, DE"
}

// RegionScheduler picks the best region for a sandbox create() request.
// Strategy: measure round-trip latency from this orchestrator to each region's
// health endpoint, then pick the lowest-latency region.
//
// In the document: "pick region by client IP RTT → pick node by available memory"
type RegionScheduler struct {
	regions []Region
	mu      sync.RWMutex
	latency map[string]time.Duration // region name → last measured RTT
}

func NewRegionScheduler(regions []Region) *RegionScheduler {
	return &RegionScheduler{
		regions: regions,
		latency: make(map[string]time.Duration),
	}
}

// PickRegion returns the name of the best region for a new sandbox.
// If a specific region is requested and valid, it is returned directly.
// Otherwise the lowest-latency region is chosen.
func (s *RegionScheduler) PickRegion(requestedRegion string) string {
	// explicit region pin — honour it
	if requestedRegion != "" && requestedRegion != "auto" {
		for _, r := range s.regions {
			if r.Name == requestedRegion {
				return r.Name
			}
		}
	}

	// auto — pick lowest latency
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.latency) == 0 {
		if len(s.regions) > 0 {
			return s.regions[0].Name // fallback: first region
		}
		return "default"
	}

	type regionLatency struct {
		name    string
		latency time.Duration
	}
	var ranked []regionLatency
	for name, lat := range s.latency {
		ranked = append(ranked, regionLatency{name, lat})
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].latency < ranked[j].latency
	})

	return ranked[0].name
}

// ProbeLoop continuously measures latency to all regions.
// Runs as a background goroutine.
func (s *RegionScheduler) ProbeLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// probe immediately on start
	s.probeAll()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.probeAll()
		}
	}
}

func (s *RegionScheduler) probeAll() {
	results := make(map[string]time.Duration)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, region := range s.regions {
		wg.Add(1)
		go func(r Region) {
			defer wg.Done()
			lat := s.probe(r.Endpoint)
			mu.Lock()
			results[r.Name] = lat
			mu.Unlock()
		}(region)
	}

	wg.Wait()

	s.mu.Lock()
	for name, lat := range results {
		s.latency[name] = lat
		logger.Debug("region probe", "region", name, "latency_ms", lat.Milliseconds())
	}
	s.mu.Unlock()
}

// probe measures the RTT to a region's health endpoint.
func (s *RegionScheduler) probe(endpoint string) time.Duration {
	start := time.Now()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(endpoint + "/health")
	if err != nil {
		return 999 * time.Second // treat unreachable as very high latency
	}
	resp.Body.Close()
	return time.Since(start)
}

// ClientRegion estimates the best region for a client based on their IP.
// Uses a simple geographic lookup — in production use a GeoIP database.
func ClientRegion(clientIP string) string {
	ip := net.ParseIP(clientIP)
	if ip == nil {
		return "auto"
	}

	// Very simplified — in production use MaxMind GeoIP2 or similar.
	// Map IP ranges to regions. This is just a placeholder.
	ipStr := ip.String()
	switch {
	case isInRange(ipStr, "10."):
		return "auto" // private IP — let latency probing decide
	default:
		return "auto"
	}
}

func isInRange(ip, prefix string) bool {
	return len(ip) >= len(prefix) && ip[:len(prefix)] == prefix
}
