package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	code := run([]string{"version"}, &out)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "infbus") {
		t.Fatalf("output %q does not contain infbus", out.String())
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
	for _, want := range []string{"gateway", "worker", "harvester"} {
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

func TestWorkerRequiresConfig(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"worker"}, &out); code == 0 {
		t.Fatal("worker without -config should fail")
	}
}
