package webhook

import (
	"net"
	"testing"
	"time"
)

func TestWebhookSignatureAndDestinationBoundaries(t *testing.T) {
	e := &Emitter{}
	body := []byte(`{"event":"test"}`)
	secret := "a-secret"
	signature := e.sign(body, []byte(secret))
	if !Verify(body, signature, secret) || Verify([]byte("changed"), signature, secret) {
		t.Fatal("signature validation failed")
	}
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "fc00::1"} {
		if publicIP(net.ParseIP(address)) {
			t.Errorf("accepted private IP %s", address)
		}
	}
	for _, url := range []string{"http://example.com", "https://127.0.0.1", "https://user:pass@example.com", "https://example.com/#secret"} {
		if e.ValidateURL(url) == nil {
			t.Errorf("accepted unsafe URL %s", url)
		}
	}
	if e.ValidateURL("https://example.com/events") != nil {
		t.Fatal("rejected HTTPS destination")
	}
	if Backoff(100) > 128*time.Second {
		t.Fatal("unbounded backoff")
	}
}

func TestReservedAddresses(t *testing.T) {
	for _, address := range []string{"100.100.100.200", "198.18.0.1", "64:ff9b::a00:1", "2002:7f00:1::"} {
		if publicIP(net.ParseIP(address)) {
			t.Fatalf("accepted reserved address %s", address)
		}
	}
}
