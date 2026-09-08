package worker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// TestAdvertisePublishesAndHeartbeats builds a Worker with a fake engine
// and a short heartbeat interval, runs it, and polls the MODELS bucket
// for the worker's own advertisement: it must appear promptly with the
// configured model list, and must be re-Put (heartbeated) periodically
// while the worker runs.
func TestAdvertisePublishesAndHeartbeats(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	eng := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 1, CompletionTokens: 1}}

	cfg := worker.Config{
		WorkerID: "w1",
		Models:   []worker.ModelConfig{{Name: "m1", Engine: "openai_http", MaxInflight: 4}},
		// Fast knobs so the test doesn't wait the production 15s/45s.
		AdvertiseEvery: 100 * time.Millisecond,
		AdvertiseTTL:   time.Second,
	}
	w := worker.New(nc, js, map[string]ibengine.Engine{"m1": eng}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() { _ = w.RunReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker not ready")
	}

	kv, err := js.KeyValue(ctx, wire.BucketModels)
	if err != nil {
		// Bucket creation happens inside the advertiser goroutine, which
		// races with RunReady's ready signal, so poll for it too.
		deadline := time.Now().Add(5 * time.Second)
		for {
			kv, err = js.KeyValue(ctx, wire.BucketModels)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("MODELS bucket never appeared: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	var entry jetstream.KeyValueEntry
	var ad wire.WorkerAd
	deadline := time.Now().Add(5 * time.Second)
	for {
		entry, err = kv.Get(ctx, "w1")
		if err == nil {
			if uerr := json.Unmarshal(entry.Value(), &ad); uerr != nil {
				t.Fatalf("unmarshal WorkerAd: %v", uerr)
			}
			if len(ad.Models) > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no MODELS entry for w1 before deadline (last err=%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(ad.Models) != 1 {
		t.Fatalf("Models = %+v, want exactly 1", ad.Models)
	}
	got := ad.Models[0]
	want := wire.WorkerAdModel{Name: "m1", Engine: "openai_http", MaxInflight: 4}
	if got != want {
		t.Errorf("Models[0] = %+v, want %+v", got, want)
	}
	if ad.StartedAt.IsZero() {
		t.Error("StartedAt is zero, want non-zero")
	}

	firstRev := entry.Revision()
	firstLastSeen := ad.LastSeen

	deadline = time.Now().Add(3 * cfg.AdvertiseEvery)
	for {
		entry, err = kv.Get(ctx, "w1")
		if err == nil && entry.Revision() > firstRev {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no second Put observed before deadline (revision stuck at %d)", firstRev)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var ad2 wire.WorkerAd
	if err := json.Unmarshal(entry.Value(), &ad2); err != nil {
		t.Fatalf("unmarshal second WorkerAd: %v", err)
	}
	if !ad2.LastSeen.After(firstLastSeen) {
		t.Errorf("LastSeen did not advance: first=%v second=%v", firstLastSeen, ad2.LastSeen)
	}
}

// TestAdvertiseBestEffortServing is an integration-level smoke check: a
// worker started with a pre-broken MODELS bucket (see
// advertise_internal_test.go's TestAdvertiseBestEffort for the
// deterministic, race-free proof that advertise()'s KV-provisioning
// failure path is actually taken) must still become ready via RunReady
// and must still serve real requests end to end. This test does NOT by
// itself prove which branch inside advertise() ran on any given
// execution — under `-race` the advertiser's single
// CreateOrUpdateKeyValue attempt can lose its race against this test's
// own ctx cancellation (via t.Cleanup) and observe ctx.Err() instead of
// the server's real rejection — it only proves the weaker but still
// useful property that RunReady's readiness and serving path are
// unaffected by advertise() regardless of why it failed.
func TestAdvertiseBestEffortServing(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx0 := context.Background()
	if _, err := js.CreateStream(ctx0, jetstream.StreamConfig{
		Name:     "KV_" + wire.BucketModels,
		Subjects: []string{"$KV." + wire.BucketModels + ".>"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("pre-create conflicting MODELS stream: %v", err)
	}

	eng := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 1, CompletionTokens: 1}}
	cfg := worker.Config{
		WorkerID:       "w1",
		Models:         []worker.ModelConfig{{Name: "m1", Engine: "openai_http", MaxInflight: 4}},
		AdvertiseEvery: 50 * time.Millisecond,
		AdvertiseTTL:   time.Second,
	}
	w := worker.New(nc, js, map[string]ibengine.Engine{"m1": eng}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- w.RunReady(ctx, ready) }()

	// RunReady must still become ready — the MODELS bucket conflict must
	// never surface as an error from RunReady's own setup path.
	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("RunReady failed even though only the MODELS bucket is broken: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunReady did not become ready before timeout")
	}

	// And the worker must genuinely still be serving: a real streamed
	// request through the (unrelated) INFERENCE stream completes
	// normally despite advertise() having failed in the background.
	l := publishAndListen(t, nc, js, "req-best-effort", time.Now().Add(time.Minute))
	msgs, err := drain(t, l)
	if err != nil {
		t.Fatalf("worker did not serve requests despite MODELS advertise failure: %v", err)
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].Kind != wire.KindDone {
		t.Fatalf("unexpected frames: %+v", msgs)
	}
}

func TestWorkerIDDefault(t *testing.T) {
	c1 := worker.Config{}
	c1.Defaults()
	if c1.WorkerID == "" {
		t.Fatal("Defaults() left WorkerID empty")
	}

	c2 := worker.Config{}
	c2.Defaults()
	if c2.WorkerID == "" {
		t.Fatal("Defaults() left WorkerID empty")
	}

	if c1.WorkerID == c2.WorkerID {
		t.Errorf("two Defaults() calls produced the same WorkerID %q, want different suffixes", c1.WorkerID)
	}
}
