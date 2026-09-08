package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// serveHarvesterResult runs serveHarvester on its own goroutine and returns
// a channel carrying its exit code, so a test can assert it actually
// returns instead of hanging forever.
func serveHarvesterResult(ctx context.Context, out *bytes.Buffer, comps ...harvesterComponent) <-chan int {
	res := make(chan int, 1)
	go func() { res <- serveHarvester(ctx, "127.0.0.1:0", out, comps...) }()
	return res
}

// TestServeHarvesterShutsDownOnContextCancel is a regression test: the
// harvester role deadlocked on wg.Wait() for every graceful-shutdown path,
// because both components return context.Canceled
// (never a value on their done channels) when their context is cancelled.
// SIGTERM is not simulated here — the supervisor body is exercised
// directly, with components whose shutdown behaviour matches
// Harvester.Run/BudgetLedger.Run's (return ctx.Err()).
func TestServeHarvesterShutsDownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan string, 2)
	comp := func(name string) harvesterComponent {
		return harvesterComponent{name: name, run: func(c context.Context) error {
			<-c.Done()
			stopped <- name
			return c.Err() // exactly what both real components return
		}}
	}
	var out bytes.Buffer
	res := serveHarvesterResult(ctx, &out, comp("harvester"), comp("budget-ledger"))

	// Let the supervisor reach its select before asking it to stop.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case code := <-res:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0; output = %q", code, out.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveHarvester did not return after context cancellation (C1 deadlock)")
	}
	for range 2 {
		select {
		case <-stopped:
		default:
			t.Fatal("a component was never asked to stop")
		}
	}
}

// TestServeHarvesterFailFast covers the other half of C1: when one
// component dies fatally, the supervisor must cancel the survivor, tear
// down HTTP, and exit 1 promptly so a supervisor restarts the process —
// previously it logged, then hung at wg.Wait() forever.
func TestServeHarvesterFailFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dead := harvesterComponent{name: "harvester", run: func(context.Context) error {
		return errors.New("clickhouse gone")
	}}
	survivor := harvesterComponent{name: "budget-ledger", run: func(c context.Context) error {
		<-c.Done()
		return c.Err()
	}}
	var out bytes.Buffer
	res := serveHarvesterResult(ctx, &out, dead, survivor)

	select {
	case code := <-res:
		if code != 1 {
			t.Fatalf("exit code = %d, want 1; output = %q", code, out.String())
		}
		if !strings.Contains(out.String(), "component failed") {
			t.Fatalf("output %q should name the component failure", out.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveHarvester did not fail fast on a dead component (C1 deadlock)")
	}
}
