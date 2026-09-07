package gateway_test

import (
	"testing"
	"time"

	"github.com/infbus/infbus/internal/gateway"
)

// TestExampleConfigParses is a golden test against the checked-in
// deploy/gateway.example.yaml: it must always parse, and its documented
// key/alias values must always mean what the README's Configuration
// section and the quickstart curl example claim they mean. A change to
// either the example file or gateway.LoadConfig that breaks this contract
// should fail here rather than surface as a confusing quickstart failure.
func TestExampleConfigParses(t *testing.T) {
	cfg, err := gateway.LoadConfig("../../deploy/gateway.example.yaml")
	if err != nil {
		t.Fatalf("LoadConfig(deploy/gateway.example.yaml): %v", err)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.RequestTimeout != 5*time.Minute {
		t.Errorf("RequestTimeout = %v, want 5m", cfg.RequestTimeout)
	}
	if len(cfg.Keys) != 1 {
		t.Fatalf("Keys = %+v, want exactly 1", cfg.Keys)
	}
	k := cfg.Keys[0]
	if k.Key != "ib_dev_change_me" || k.Name != "dev" || k.Org != "dev-org" || k.Project != "default" {
		t.Errorf("Keys[0] = %+v", k)
	}
	if len(k.Allow) != 1 || k.Allow[0] != "fast" {
		t.Errorf("Keys[0].Allow = %v, want [fast]", k.Allow)
	}
	if got, want := cfg.Aliases["fast"], "llama3.2"; got != want {
		t.Errorf("Aliases[fast] = %q, want %q", got, want)
	}
}
