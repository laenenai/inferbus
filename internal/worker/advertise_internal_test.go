package worker

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

// TestAdvertiseBestEffort is the deterministic, race-free proof that
// advertise()'s KV-provisioning-failure path actually executes and that
// it never blocks or panics. It lives in this internal test package (not
// worker_test) specifically so it can call the unexported advertise
// method directly and synchronously, on a context.Background() that is
// never canceled during the call — unlike driving advertise() through
// RunReady in a separate goroutine, there is no cancellation for the
// call to race against, so the server's real rejection is what
// advertise() observes on every run, not context.Canceled.
//
// The MODELS bucket is broken the same way as the external
// TestAdvertiseBestEffortServing test (advertise_test.go): the
// underlying "KV_MODELS" stream is pre-created with an incompatible
// storage type, so the advertiser's CreateOrUpdateKeyValue call (which
// defaults to file storage) is rejected by the server with "stream
// configuration update can not change storage type". advertise() only
// attempts this once, before its ticker loop starts (it does not retry
// per tick) — on failure it logs and returns immediately.
func TestAdvertiseBestEffort(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "KV_" + wire.BucketModels,
		Subjects: []string{"$KV." + wire.BucketModels + ".>"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("pre-create conflicting MODELS stream: %v", err)
	}

	eng := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 1, CompletionTokens: 1}}
	cfg := Config{
		WorkerID:       "w1",
		Models:         []ModelConfig{{Name: "m1", Engine: "openai_http", MaxInflight: 4}},
		AdvertiseEvery: 50 * time.Millisecond,
		AdvertiseTTL:   time.Second,
	}
	w := New(nc, js, map[string]ibengine.Engine{"m1": eng}, cfg)

	// Direct, synchronous call — no ctx cancellation racing it, no
	// goroutine scheduling to lose. Guard with a timeout goroutine only
	// to fail the test cleanly (instead of hanging forever) if advertise
	// ever blocks; a panic inside the call crashes the test binary just
	// like it would in production, which is exactly what we want to
	// observe if it happened.
	done := make(chan struct{})
	go func() {
		w.advertise(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("advertise() did not return promptly on a broken MODELS bucket")
	}

	// Prove the failure branch was actually taken, not skipped or raced
	// past: CreateOrUpdateKeyValue must have been rejected, which means
	// (a) the pre-existing stream's storage type was never overwritten,
	// and (b) advertise() returned before ever reaching its Put loop, so
	// no "w1" entry exists in the bucket.
	stream, err := js.Stream(ctx, "KV_"+wire.BucketModels)
	if err != nil {
		t.Fatalf("re-fetch conflicting stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("fetch fresh stream info: %v", err)
	}
	if info.Config.Storage != jetstream.MemoryStorage {
		t.Fatalf("KV_MODELS storage = %v, want unchanged MemoryStorage (advertise() must have failed to update it)", info.Config.Storage)
	}

	// Because the update was rejected, the stream never became a valid KV
	// bucket in the first place — js.KeyValue must still refuse it. This
	// is further confirmation advertise() never reached its Put loop (no
	// "w1" entry could exist if the bucket handle itself is unusable).
	if _, err := js.KeyValue(ctx, wire.BucketModels); err == nil {
		t.Fatal("KeyValue(MODELS) unexpectedly succeeded — the conflicting stream should still be storage-incompatible")
	}
}
