package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/testutil"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// TestHealth_ComposesRelayAndProjectorFailure covers the health type
// introduced for review findings C1/I1/I2: it must report unhealthy the
// instant EITHER the (never-restarted) relay fails OR the CURRENT
// projector generation fails, and a resync's setProjFailed swap must
// clear a stale failure from a previous generation.
func TestHealth_ComposesRelayAndProjectorFailure(t *testing.T) {
	relayFailed := make(chan struct{})
	h := newHealth(relayFailed)
	if !h.ok() {
		t.Fatal("fresh health should be ok")
	}

	gen1 := make(chan struct{})
	h.setProjFailed(gen1)
	if !h.ok() {
		t.Fatal("health with an open projFailed channel should be ok")
	}

	close(gen1)
	if h.ok() {
		t.Fatal("health should be unhealthy once the current projector generation failed")
	}

	// Swapping in a new generation (as resyncFn does after a successful
	// restart) clears the old failure.
	gen2 := make(chan struct{})
	h.setProjFailed(gen2)
	if !h.ok() {
		t.Fatal("health should be healthy again after swapping in a fresh projector generation")
	}

	close(relayFailed)
	if h.ok() {
		t.Fatal("health should be unhealthy once the relay failed, regardless of projector generation")
	}
}

// bareStore strips a concrete es.Store down to exactly the es.Store
// interface's method set, hiding whatever delivery.Drainer/
// delivery.Checkpoints the concrete type (e.g. sqlite.Store) actually
// implements — used below to force RunRelay's "neither" config-error path
// deterministically.
type bareStore struct{ es.Store }

// TestRunRelay_ConfigErrorClosesFailedChannel is review finding C1's other
// half: an immediate config-error return from RunRelay (as opposed to a
// later fail-stop) must close runRelay's failed channel just as promptly
// as a genuine runtime failure would — Run()'s main select relies on this
// to shut the process down instead of quietly serving with a dead relay.
func TestRunRelay_ConfigErrorClosesFailedChannel(t *testing.T) {
	ctx := context.Background()
	real, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { real.Close() })

	_, js := testutil.RunNATS(t)

	cancel, stopped, failed := runRelay(ctx, bareStore{real}, js)
	t.Cleanup(cancel)

	select {
	case <-failed:
		// Expected: bareStore implements neither delivery.Drainer nor
		// delivery.Checkpoints, so RunRelay returns an error immediately.
	case <-time.After(5 * time.Second):
		t.Fatal("runRelay's failed channel did not close after RunRelay's immediate config-error return")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("runRelay's stopped channel did not close")
	}
}
