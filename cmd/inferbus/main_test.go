package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	code := run([]string{"version"}, &out)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "inferbus") {
		t.Fatalf("output %q does not contain inferbus", out.String())
	}
}

func TestRunUnknownSubcommand(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"bogus"}, &out); code == 0 {
		t.Fatal("unknown subcommand should not exit 0")
	}
}

func TestRunNoArgsShowsUsage(t *testing.T) {
	var out bytes.Buffer
	if code := run(nil, &out); code == 0 {
		t.Fatal("no args should not exit 0")
	}
	for _, want := range []string{"gateway", "worker", "harvester", "controlplane"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("usage %q missing %q", out.String(), want)
		}
	}
}

func TestGatewayRequiresConfig(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"gateway"}, &out); code == 0 {
		t.Fatal("gateway without -config should fail")
	}
	if !strings.Contains(out.String(), "-config") {
		t.Fatalf("output %q should mention -config", out.String())
	}
}

// TestGatewayKVModeWithStaticKeysFailsFast covers I3's roles-level
// requirement: `iam.mode: kv` combined with a non-empty `keys:` list must
// exit 1 with a message naming the conflict, and must do so WITHOUT ever
// touching NATS (this test runs with no NATS server reachable at all —
// if the guard moved after the NATS dial, this test would instead fail
// with a connection-refused message, not the intended one).
func TestGatewayKVModeWithStaticKeysFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	contents := "iam:\n  mode: kv\nkeys:\n  - key: ib_dev_change_me\n    name: dev\n    allow: [fast]\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var out bytes.Buffer
	code := run([]string{"gateway", "-config", path}, &out)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; output = %q", code, out.String())
	}
	if !strings.Contains(out.String(), "iam.mode") || !strings.Contains(out.String(), "kv") || !strings.Contains(out.String(), "keys") {
		t.Fatalf("output %q should explain the iam.mode=kv + keys: conflict", out.String())
	}
}

func TestWorkerRequiresConfig(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"worker"}, &out); code == 0 {
		t.Fatal("worker without -config should fail")
	}
}

func TestControlplaneRequiresConfig(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"controlplane"}, &out); code == 0 {
		t.Fatal("controlplane without -config should fail")
	}
	if !strings.Contains(out.String(), "-config") {
		t.Fatalf("output %q should mention -config", out.String())
	}
}

func TestHarvesterRequiresConfig(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"harvester"}, &out); code == 0 {
		t.Fatal("harvester without -config should fail")
	}
	if !strings.Contains(out.String(), "-config") {
		t.Fatalf("output %q should mention -config", out.String())
	}
}
