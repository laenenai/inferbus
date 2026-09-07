package gateway_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
)

// createBucket creates bucket (idempotently) directly against js — tests
// feed KVIAM's maps with raw Puts against the same ALIASES/KEYS schema
// controlplane's projectors write (kvproj.go, now internal/cpkv per the
// I5 layering ruling), never through the projectors themselves, per the
// task brief ("KVIAM map behavior fed by direct bucket Puts").
func createBucket(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValue {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: name})
	if err != nil {
		t.Fatalf("create bucket %q: %v", name, err)
	}
	return kv
}

func putKeyEntry(t *testing.T, kv jetstream.KeyValue, hash string, entry cpkv.KeyEntry) {
	t.Helper()
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(context.Background(), hash, b); err != nil {
		t.Fatalf("put key entry %q: %v", hash, err)
	}
}

func putAliasEntry(t *testing.T, kv jetstream.KeyValue, key string, entry cpkv.AliasEntry) {
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

// waitReady blocks until kv.Ready() closes or timeout elapses (failing the
// test on timeout). Review ruling I6 made NewKVIAM non-blocking — it
// starts its watch loops and returns immediately, with no guarantee
// either bucket has been scanned yet — so every test that used to rely on
// the old blocking constructor for synchronization now waits on Ready()
// explicitly instead.
func waitReady(t *testing.T, kv *gateway.KVIAM, timeout time.Duration) {
	t.Helper()
	select {
	case <-kv.Ready():
	case <-time.After(timeout):
		t.Fatal("KVIAM did not become ready before timeout")
	}
}

// newTestKVIAM constructs a KVIAM against js and waits for it to become
// ready (both buckets have their first snapshot) before returning. Tests
// that only care about one of the two buckets must still ensure the
// other exists (createBucket is idempotent) or Ready() would never close.
func newTestKVIAM(t *testing.T, js jetstream.JetStream) *gateway.KVIAM {
	t.Helper()
	createBucket(t, js, cpkv.BucketKeys)
	createBucket(t, js, cpkv.BucketAliases)
	// ctx must outlive this constructor call — NewKVIAM's watch goroutines
	// keep running (and must, to observe live updates) for as long as ctx
	// stays alive, well past NewKVIAM itself returning. Canceling on
	// return here (a bare `defer cancel()`) would kill the watchers the
	// instant construction finished, before any test got to see a live
	// update; t.Cleanup ties cancellation to the end of the test instead.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	kv, err := gateway.NewKVIAM(ctx, js)
	if err != nil {
		t.Fatalf("NewKVIAM: %v", err)
	}
	waitReady(t, kv, 10*time.Second)
	return kv
}

// TestKVIAM_AuthenticateKey_HitAndMiss covers the base case: a KEYS entry
// put directly into the bucket (as controlplane's keys projector would)
// authenticates by its plaintext's hash, an unrelated plaintext misses,
// and the returned KeyConfig carries the row's Allow/Org/Project/Name
// (usage attribution + allowlist checks downstream depend on these).
func TestKVIAM_AuthenticateKey_HitAndMiss(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, cpkv.BucketKeys)

	plaintext := "ib_live_test_key_1"
	hash := cpkv.HashKey(plaintext)
	putKeyEntry(t, kv, hash, cpkv.KeyEntry{
		Org: "acme", Project: "prod", Name: "k1", Allow: []string{"fast", "smart"},
	})

	iam := newTestKVIAM(t, js)

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
	kv := createBucket(t, js, cpkv.BucketKeys)

	plaintext := "ib_live_test_key_2"
	hash := cpkv.HashKey(plaintext)
	putKeyEntry(t, kv, hash, cpkv.KeyEntry{Org: "acme", Project: "prod", Name: "k2", Allow: []string{"fast"}})

	iam := newTestKVIAM(t, js)

	if _, ok := iam.AuthenticateKey(plaintext); !ok {
		t.Fatal("expected auth hit before deletion")
	}

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
	kv := createBucket(t, js, cpkv.BucketAliases)

	putAliasEntry(t, kv, "_global/fast", cpkv.AliasEntry{Target: "global-model"})
	putAliasEntry(t, kv, "acme/fast", cpkv.AliasEntry{Target: "acme-model"})

	iam := newTestKVIAM(t, js)

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

// TestKVIAM_MissingBucketRetriesUntilCreated covers binding ruling #2 and
// review ruling I6 together: a gateway started in kv mode before the
// control plane has ever created the buckets must not fail permanently.
// NewKVIAM itself returns immediately (I6 — no blocking scans), so this
// test asserts on Ready(): it must not close until the buckets actually
// exist, and must close once they do (bounded backoff retry, not a
// permanent failure).
func TestKVIAM_MissingBucketRetriesUntilCreated(t *testing.T) {
	_, js := testutil.RunNATS(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	kv, err := gateway.NewKVIAM(ctx, js)
	if err != nil {
		t.Fatalf("NewKVIAM: %v", err)
	}

	// NewKVIAM must have returned immediately (I6), well before either
	// bucket exists — Ready() must not have closed yet.
	select {
	case <-kv.Ready():
		t.Fatal("KVIAM reported ready before either bucket existed")
	default:
	}

	// Give the watch loops a moment to observe both buckets as missing at
	// least once before they appear — this is a real sleep, but it is not
	// the synchronization primitive (waitReady below is); it just
	// improves the odds this test actually exercises the retry path
	// rather than a lucky race where the buckets already existed by the
	// time the first attempt ran.
	time.Sleep(100 * time.Millisecond)

	createBucket(t, js, cpkv.BucketKeys)
	createBucket(t, js, cpkv.BucketAliases)

	waitReady(t, kv, 15*time.Second)
	if _, ok := kv.AuthenticateKey("anything"); ok {
		t.Fatal("expected a miss against an empty KEYS bucket")
	}
}

// TestKVIAM_SurvivesBucketDestroyAndRecreate covers binding ruling #1 and
// review ruling C1: controlplane.ResyncKV destroys and recreates the
// ALIASES/KEYS buckets wholesale (kvproj.go's resetKVBucket), which kills
// whatever KV watcher was subscribed to the old one. KVIAM must notice
// the recreated bucket via its identity probe and re-scan/re-watch it,
// rather than serving a stale-forever map or crashing the gateway.
func TestKVIAM_SurvivesBucketDestroyAndRecreate(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, cpkv.BucketKeys)

	plaintext1 := "ib_live_before_resync"
	hash1 := cpkv.HashKey(plaintext1)
	putKeyEntry(t, kv, hash1, cpkv.KeyEntry{Org: "acme", Project: "prod", Name: "before", Allow: []string{"fast"}})

	iam := newTestKVIAM(t, js)
	if _, ok := iam.AuthenticateKey(plaintext1); !ok {
		t.Fatal("expected auth hit before resync")
	}

	// Simulate controlplane.ResyncKV's destroy+recreate (kvproj.go's
	// resetKVBucket): delete the bucket outright, which tears down its
	// underlying JetStream stream and, with it, any live watch — then
	// immediately recreate it empty and put a different entry, as a
	// resync's replay would.
	//
	// Note on mechanism (review ruling C1): watchKV no longer relies on
	// the watcher's own Updates() channel closing to notice this (the
	// underlying nats.go ordered-consumer watcher can take anywhere from
	// ~10s to ~40s to notice a destroyed stream on its own, gated by its
	// internal consumer-heartbeat/idle detection — far too slow and too
	// variable to build a staleness bound on). Instead a periodic identity
	// probe (kviam.go's kvProbeInterval, 5s) compares the bucket's backing
	// stream's Created timestamp and forces a reconnect the moment it
	// observes a change, without ever interrupting an otherwise-healthy,
	// in-progress watch. This test's poll window below is sized against
	// that probe interval.
	if err := js.DeleteKeyValue(context.Background(), cpkv.BucketKeys); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	kv2 := createBucket(t, js, cpkv.BucketKeys)
	plaintext2 := "ib_live_after_resync"
	hash2 := cpkv.HashKey(plaintext2)
	putKeyEntry(t, kv2, hash2, cpkv.KeyEntry{Org: "acme", Project: "prod", Name: "after", Allow: []string{"fast"}})

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

// TestKVIAM_StableBucketNeverReconnects covers the C1 review finding from
// the opposite direction of TestKVIAM_SurvivesBucketDestroyAndRecreate: a
// bucket whose identity never changes must not be torn down and
// reconnected at all, no matter how long the watch has been running. The
// old blind-timer design (replaced per ruling C1) forced a reconnect
// every kvWatchRefreshInterval regardless of health — which both
// risked livelocking a slow initial replay and, more simply, was pure
// unnecessary churn for the overwhelmingly common case of "nothing
// happened." This test lets a watch run for comfortably longer than
// kvProbeInterval (5s) with a completely stable bucket, and asserts the
// entry put before that wait is still visible afterward — the mere fact
// that this passes doesn't distinguish "reconnected cleanly" from "never
// reconnected," but combined with the resync test (which proves a
// reconnect DOES happen when identity changes) it establishes that
// reconnects are conditional on identity, not on elapsed time.
func TestKVIAM_StableBucketNeverReconnects(t *testing.T) {
	_, js := testutil.RunNATS(t)
	kv := createBucket(t, js, cpkv.BucketKeys)

	plaintext := "ib_live_stable"
	hash := cpkv.HashKey(plaintext)
	putKeyEntry(t, kv, hash, cpkv.KeyEntry{Org: "acme", Project: "prod", Name: "stable", Allow: []string{"fast"}})

	iam := newTestKVIAM(t, js)
	if _, ok := iam.AuthenticateKey(plaintext); !ok {
		t.Fatal("expected auth hit immediately after ready")
	}

	time.Sleep(7 * time.Second) // comfortably past kvProbeInterval (5s)

	if _, ok := iam.AuthenticateKey(plaintext); !ok {
		t.Fatal("entry disappeared after an idle period past the probe interval — a stable bucket must never lose its snapshot")
	}
}
