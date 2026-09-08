package gateway_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/gateway"
)

// writeConfig writes contents to a temp file and returns its path — used
// by the IAM-mode parsing tests below, which each need a small,
// purpose-built config rather than the checked-in example file.
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

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
	if cfg.IAM.Mode != "static" {
		t.Errorf("IAM.Mode = %q, want %q (default, no iam: block in the example)", cfg.IAM.Mode, "static")
	}
}

// TestConfigDefaultsToStaticIAM covers I3's config-parsing requirement: a
// config with no iam: block at all (not just the checked-in example)
// still defaults Mode to "static" — the only mode M2 ever had, and the
// only one that keeps working with zero config changes.
func TestConfigDefaultsToStaticIAM(t *testing.T) {
	path := writeConfig(t, "keys:\n  - key: k1\n    name: n1\n    allow: [fast]\naliases:\n  fast: m1\n")
	cfg, err := gateway.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.IAM.Mode != "static" {
		t.Errorf("IAM.Mode = %q, want %q", cfg.IAM.Mode, "static")
	}
}

// TestConfigParsesExplicitKVMode covers I3's other required case: an
// explicit `iam: {mode: kv}` block parses to Mode "kv" (and is not
// silently overwritten by LoadConfig's static default, which only fires
// when Mode is the empty string).
func TestConfigParsesExplicitKVMode(t *testing.T) {
	path := writeConfig(t, "iam:\n  mode: kv\n")
	cfg, err := gateway.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.IAM.Mode != "kv" {
		t.Errorf("IAM.Mode = %q, want %q", cfg.IAM.Mode, "kv")
	}
}

// TestKVExampleConfigParses is the golden test for
// deploy/gateway.kv.example.yaml, the config the compose stack mounts
// (a static gateway config would mean the README's
// budgets quickstart could never produce the 402 it documents — 402
// enforcement exists only in kv mode). It must parse, be kv mode, and
// carry no static keys — the gateway refuses to start if a kv config also
// lists keys.
func TestKVExampleConfigParses(t *testing.T) {
	cfg, err := gateway.LoadConfig("../../deploy/gateway.kv.example.yaml")
	if err != nil {
		t.Fatalf("LoadConfig(deploy/gateway.kv.example.yaml): %v", err)
	}
	if cfg.IAM.Mode != "kv" {
		t.Errorf("IAM.Mode = %q, want kv", cfg.IAM.Mode)
	}
	if len(cfg.Keys) != 0 {
		t.Errorf("Keys = %+v, want none in a kv-mode config", cfg.Keys)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.RequestTimeout != 5*time.Minute {
		t.Errorf("RequestTimeout = %v, want 5m", cfg.RequestTimeout)
	}
}
