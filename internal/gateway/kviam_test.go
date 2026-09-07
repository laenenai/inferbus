package gateway_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
)

// createBucket creates bucket (idempotently) directly against js — tests
// feed KVIAM's maps with raw Puts against the same ALIASES/KEYS schema
// controlplane's projectors write (kvproj.go), never through the
// projectors themselves, per the task brief ("KVIAM map behavior fed by
// direct bucket Puts").
func createBucket(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValue {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: name})
	if err != nil {
		t.Fatalf("create bucket %q: %v", name, err)
	}
	return kv
}

func putKeyEntry(t *testing.T, kv jetstream.KeyValue, hash string, entry controlplane.KeyEntry) {
	t.Helper()
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(context.Background(), hash, b); err != nil {
		t.Fatalf("put key entry %q: %v", hash, err)
	}
}

func putAliasEntry(t *testing.T, kv jetstream.KeyValue, key string, entry controlplane.AliasEntry) {
	t.Helper()
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(context.Background(), key, b); err != nil {
		t.Fatalf("put alias entry %q: %v", key, err)
	}
}

// pollUntil polls check every 20ms until it returns true or the deadline
// elapses, failing the test on timeout. This is the codebase's
// established poll-based wait idiom (see e.g.
// internal/gateway/gateway_test.go's
// TestNoWorkerConsumerDeadlineCleansUpQueuedMessage) — never a bare sleep
// as the sole synchronization for an async watcher update.
func pollUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// newTestKVIAM constructs a KVIAM against js. NewKVIAM blocks until BOTH
// the KEYS and ALIASES buckets have completed their first scan, so tests
// that only care about one of the two must still ensure the other exists
// (createBucket is idempotent) or NewKVIAM would block on the missing one
// until ctx's deadline.
func newTestKVIAM(t *testing.T, js jetstream.JetStream) *gateway.KVIAM {
	t.Helper()
	createBucket(t, js, controlplane.BucketKeys)
	createBucket(t, js, controlplane.BucketAliases)
	// ctx must outlive this constructor call — NewKVIAM's watch goroutines
	// keep running (and must, to observe live updates) for as long as ctx
	// stays alive, well past NewKVIAM itself returning. Canceling on
	// return here (a bare `defer cancel()`) would kill the watchers the
	// instant construction finished, before any test got to see a live
	// update; t.Cleanup ties cancellation to the end of the test instead.
	// Both buckets already exist by the time NewKVIAM is called, so its
	// blocking initial-scan phase returns near-instantly; the test
	// binary's own default `go test` timeout is the backstop if that
	// assumption is ever wrong.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	kv, err := gateway.NewKVIAM(ctx, js)
	if err != nil {
		t.Fatalf("NewKVIAM: %v", err)
	}
	return kv
}

// TestKVIAM_AuthenticateKey_HitAndMiss covers the base case: a KEYS entry
// put directly into the bucket (as controlplane's keys projector would)
// authenticates by its plaintext's hash, an unrelated plaintext misses,
// and the returned KeyConfig carries the row's Allow/Org/Project/Name
// (usage attribution + allowlist checks downstream depend on these).
func TestKVIAM_AuthenticateKey_HitAndMiss(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, controlplane.BucketKeys)

	plaintext := "ib_live_test_key_1"
	hash := controlplane.HashKey(plaintext)
	putKeyEntry(t, kv, hash, controlplane.KeyEntry{
		Org: "acme", Project: "prod", Name: "k1", Allow: []string{"fast", "smart"},
	})

	iam := newTestKVIAM(t, js)

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := iam.AuthenticateKey(plaintext)
		return ok
	})

	got, ok := iam.AuthenticateKey(plaintext)
	if !ok {
		t.Fatal("expected auth hit for known plaintext")
	}
	if got.Org != "acme" || got.Project != "prod" || got.Name != "k1" {
		t.Fatalf("KeyConfig = %+v", got)
	}
	if len(got.Allow) != 2 || got.Allow[0] != "fast" || got.Allow[1] != "smart" {
		t.Fatalf("KeyConfig.Allow = %v", got.Allow)
	}

	if _, ok := iam.AuthenticateKey("ib_live_never_issued"); ok {
		t.Fatal("expected auth miss for unknown plaintext")
	}
}

// TestKVIAM_DeletionDeniesAfterWatchUpdate covers binding ruling #1: a
// disabled key is deleted from the KEYS bucket entirely (never flagged),
// so absence must mean deny — and KVIAM's live watcher, not just its
// initial scan, must observe that deletion.
func TestKVIAM_DeletionDeniesAfterWatchUpdate(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, controlplane.BucketKeys)

	plaintext := "ib_live_test_key_2"
	hash := controlplane.HashKey(plaintext)
	putKeyEntry(t, kv, hash, controlplane.KeyEntry{Org: "acme", Project: "prod", Name: "k2", Allow: []string{"fast"}})

	iam := newTestKVIAM(t, js)

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := iam.AuthenticateKey(plaintext)
		return ok
	})

	if err := kv.Delete(context.Background(), hash); err != nil {
		t.Fatalf("delete key entry: %v", err)
	}

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := iam.AuthenticateKey(plaintext)
		return !ok
	})
}

// TestKVIAM_ResolveAlias_OrgOverridesGlobal covers ResolveAlias's
// documented precedence: an org-scoped alias entry wins over a
// same-named global one, and an org with no override at all falls
// through to the global entry.
func TestKVIAM_ResolveAlias_OrgOverridesGlobal(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, controlplane.BucketAliases)

	putAliasEntry(t, kv, "_global/fast", controlplane.AliasEntry{Target: "global-model"})
	putAliasEntry(t, kv, "acme/fast", controlplane.AliasEntry{Target: "acme-model"})

	iam := newTestKVIAM(t, js)

	pollUntil(t, 5*time.Second, func() bool {
		target, ok := iam.ResolveAlias("acme", "fast")
		return ok && target == "acme-model"
	})

	target, ok := iam.ResolveAlias("acme", "fast")
	if !ok || target != "acme-model" {
		t.Fatalf("ResolveAlias(acme, fast) = (%q, %v), want (acme-model, true)", target, ok)
	}

	// A different org with no override at all falls back to the global
	// entry.
	target, ok = iam.ResolveAlias("other-org", "fast")
	if !ok || target != "global-model" {
		t.Fatalf("ResolveAlias(other-org, fast) = (%q, %v), want (global-model, true)", target, ok)
	}

	// An unknown alias in any scope misses cleanly.
	if _, ok := iam.ResolveAlias("acme", "nope"); ok {
		t.Fatal("expected miss for unknown alias")
	}
}

// TestKVIAM_MissingBucketRetriesUntilCreated covers binding ruling #2: a
// gateway started in kv mode before the control plane has ever created
// the buckets must not fail permanently — NewKVIAM polls with backoff
// until the bucket appears (or ctx is done), so a caller that creates the
// bucket a little late still gets a working KVIAM.
func TestKVIAM_MissingBucketRetriesUntilCreated(t *testing.T) {
	_, js := testutil.RunNATS(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	type result struct {
		kv  *gateway.KVIAM
		err error
	}
	done := make(chan result, 1)
	go func() {
		kv, err := gateway.NewKVIAM(ctx, js)
		done <- result{kv: kv, err: err}
	}()

	// Give NewKVIAM a moment to observe the bucket as missing at least
	// once before it appears — this is a real sleep, but it is not the
	// synchronization primitive (the `done` channel and pollUntil below
	// are); it just improves the odds this test actually exercises the
	// retry path rather than a lucky race where the bucket already
	// existed on the very first attempt.
	time.Sleep(100 * time.Millisecond)

	createBucket(t, js, controlplane.BucketKeys)
	createBucket(t, js, controlplane.BucketAliases)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("NewKVIAM: %v", r.err)
		}
		if _, ok := r.kv.AuthenticateKey("anything"); ok {
			t.Fatal("expected a miss against an empty KEYS bucket")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("NewKVIAM did not return after the buckets were created")
	}
}

// TestKVIAM_SurvivesBucketDestroyAndRecreate covers binding ruling #1
// directly: controlplane.ResyncKV destroys and recreates the ALIASES/KEYS
// buckets wholesale (kvproj.go's resetKVBucket), which kills whatever KV
// watcher was subscribed to the old one. KVIAM must notice its watcher
// died and re-scan/re-watch the recreated bucket, rather than serving a
// stale-forever map or crashing the gateway.
func TestKVIAM_SurvivesBucketDestroyAndRecreate(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, controlplane.BucketKeys)

	plaintext1 := "ib_live_before_resync"
	hash1 := controlplane.HashKey(plaintext1)
	putKeyEntry(t, kv, hash1, controlplane.KeyEntry{Org: "acme", Project: "prod", Name: "before", Allow: []string{"fast"}})

	iam := newTestKVIAM(t, js)
	pollUntil(t, 5*time.Second, func() bool {
		_, ok := iam.AuthenticateKey(plaintext1)
		return ok
	})

	// Simulate controlplane.ResyncKV's destroy+recreate (kvproj.go's
	// resetKVBucket): delete the bucket outright, which tears down its
	// underlying JetStream stream and, with it, any live watch — then
	// immediately recreate it empty and put a different entry, as a
	// resync's replay would.
	//
	// Note on timing: the underlying nats.go ordered-consumer watcher does
	// not notice a deleted stream promptly on its own — a standalone
	// probe against this same nats.go version measured anywhere from
	// ~10s to ~40s before its Updates() channel actually closed, gated by
	// the client's own consumer-heartbeat/idle detection. watchKV does
	// not rely on that: kvWatchRefreshInterval (kviam.go) forces a fresh
	// scan+watch every 5s regardless, which is what this test's poll
	// window below is sized against — not the client's own (much slower
	// and less predictable) passive detection.
	if err := js.DeleteKeyValue(context.Background(), controlplane.BucketKeys); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	kv2 := createBucket(t, js, controlplane.BucketKeys)
	plaintext2 := "ib_live_after_resync"
	hash2 := controlplane.HashKey(plaintext2)
	putKeyEntry(t, kv2, hash2, controlplane.KeyEntry{Org: "acme", Project: "prod", Name: "after", Allow: []string{"fast"}})

	pollUntil(t, 15*time.Second, func() bool {
		_, ok := iam.AuthenticateKey(plaintext2)
		return ok
	})

	// By the time the recreated bucket's sole entry is visible, watchKV
	// must have done a full from-scratch re-scan (consumeKV always starts
	// working from an empty map on every (re)connect) — so the pre-resync
	// key is necessarily gone from the map too, not merely racing to
	// expire on its own.
	if _, ok := iam.AuthenticateKey(plaintext1); ok {
		t.Fatal("pre-resync key must not authenticate against the recreated bucket")
	}
}
