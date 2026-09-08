// iamProvider is the seam introduced by Task 11 between the gateway's
// request path (authenticate + alias resolution in gateway.go) and however
// key/alias authority is actually sourced. Two implementations exist:
//
//   - staticIAM wraps the pre-existing static Config (M2 behavior,
//     unchanged) — the default, and what every pre-Task-11 gateway test
//     still exercises.
//   - KVIAM sources both maps live from the control plane's ALIASES/KEYS
//     NATS KV buckets (Tasks 7/10), for `iam.mode: kv` deployments. It
//     depends only on internal/cpkv (the shared bucket-name/JSON-schema/
//     hashing package, I5 review ruling) — never on internal/controlplane
//     itself, which would drag Postgres and OIDC dependencies into a
//     gateway binary that never uses either.
//
// Both satisfy the same two methods, so gateway.go's chatCompletions/models
// handlers never need to know which one they're talking to.
package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
)

// iamProvider is the gateway's key-auth + alias-resolution authority.
// AuthenticateKey takes the raw, still-unhashed presented credential (the
// bearer token as the client sent it) and returns the matching key's
// config plus whether it matched at all. ResolveAlias tries an
// org-scoped alias first, falling back to the global scope.
type iamProvider interface {
	AuthenticateKey(presented string) (KeyConfig, bool)
	ResolveAlias(org, alias string) (target string, ok bool)
}

// staticIAM adapts the M2 static Config to iamProvider. Its
// AuthenticateKey is the exact constant-time compare gateway.go's
// authenticate used to do inline; ResolveAlias ignores org (the static
// config has no notion of org-scoped aliases — a single flat map serves
// every key).
type staticIAM struct {
	cfg Config
}

func newStaticIAM(cfg Config) *staticIAM { return &staticIAM{cfg: cfg} }

func (s *staticIAM) AuthenticateKey(presented string) (KeyConfig, bool) {
	pb := []byte(presented)
	for _, k := range s.cfg.Keys {
		if subtle.ConstantTimeCompare(pb, []byte(k.Key)) == 1 {
			return k, true
		}
	}
	return KeyConfig{}, false
}

func (s *staticIAM) ResolveAlias(_, alias string) (string, bool) {
	target, ok := s.cfg.Aliases[alias]
	return target, ok
}

// kvWatchMinBackoff/kvWatchMaxBackoff bound the retry delay a KVIAM watch
// loop uses between a failed bucket lookup/watch attempt and the next one
// (binding ruling #1/#2: bounded exponential backoff, not a hot loop, and
// not a permanent failure). Per review ruling I1, the delay resets to the
// minimum only once a connection attempt actually reaches a successful
// first sync (see watchKV) — not merely because WatchAll itself returned
// without error — so a watch that connects but dies again before ever
// finishing its initial replay keeps climbing the backoff instead of
// resetting on every brief, failing connection.
const (
	kvWatchMinBackoff = 200 * time.Millisecond
	kvWatchMaxBackoff = 10 * time.Second
)

// kvProbeInterval is how often watchKV compares the watched bucket's
// backing JetStream stream identity (its Created timestamp) against the
// value captured when the current watch connected (review ruling C1).
//
// This replaces an earlier design that blindly tore down and reconnected
// every watch on a fixed timer regardless of health. That design could
// livelock: if a single initial replay legitimately takes longer than the
// timer, the timer cancels watchCtx out from under an in-progress,
// otherwise-healthy scan, and if every subsequent attempt takes just as
// long, KVIAM never finishes a scan at all — Ready() would never close.
// The probe instead only forces a reconnect when the bucket's underlying
// stream has actually been destroyed and recreated (controlplane.ResyncKV's
// resetKVBucket — the one real-world case this mechanism exists for) and
// otherwise never interferes with an in-progress watch, however long it
// runs. It compares stream identity via "KV_<bucket>"'s StreamInfo.Created:
// destroy+recreate always produces a new stream with a new Created time,
// even if the recreation happens within the same probe interval.
const kvProbeInterval = 5 * time.Second

// kvLogAlwaysUntil/kvLogEveryFailures rate-limit the warn-level logging of
// repeated KeyValue/stream-info/WatchAll connect failures (review ruling
// I2): the first few attempts always log (an operator watching a fresh
// deploy wants to see trouble immediately), then only every
// kvLogEveryFailures'th attempt after that — otherwise a bucket that stays
// missing for a long time (e.g. the control plane never starts) logs a
// warning forever, once per backoff interval (at most every
// kvWatchMaxBackoff, but that is still unbounded over time).
const (
	kvLogAlwaysUntil   = 3
	kvLogEveryFailures = 10
)

// truncateHash returns at most the first 8 characters of s. Every KEYS
// bucket key is a SHA-256 hash of a real API key credential; review ruling
// I2 requires that no key material ever reaches a log line, and that any
// hash that does get logged (for debugging/correlation purposes) is
// truncated. 8 hex characters is enough to correlate log lines with a
// specific bucket entry without printing anything close to a usable prefix
// of the underlying hash.
func truncateHash(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// KVIAM is the control-plane-backed iamProvider (Task 11): it scans and
// live-watches the KEYS and ALIASES KV buckets (cpkv.BucketKeys/
// BucketAliases) into two atomically-swapped maps, so the gateway's hot
// request path (AuthenticateKey/ResolveAlias) never itself touches NATS —
// every request reads an already-materialized snapshot.
//
// Resilience (binding ruling #1): the ALIASES/KEYS buckets are destroyed
// and recreated wholesale during an admin resync (controlplane.ResyncKV),
// which kills any KV watcher subscribed to them. KVIAM's watch loop
// notices this — either because its watcher's update channel closes, or
// because its periodic bucket-identity probe (kvProbeInterval, see its own
// doc comment / review ruling C1) detects the backing stream was recreated
// — and responds by re-resolving the bucket and re-watching from scratch,
// never by crashing the gateway or serving a stale-forever map. A
// KEYS/ALIASES entry that no longer exists (a disabled key is deleted
// upstream, per kvproj.go) is absent from the map, and absence is deny —
// there is no separate "disabled" flag to check on this side.
//
// Missing bucket at startup (binding ruling #2): the control plane may
// start after the gateway (e.g. in compose). NewKVIAM does not treat a
// missing bucket as fatal; each watch loop polls with backoff until the
// bucket appears or ctx is done.
//
// Bind-before-ready (review ruling I6): NewKVIAM itself never blocks —
// it starts both watch loops and returns immediately. Ready() reports when
// the first snapshot of both buckets has actually landed; gateway.go's
// request path consults it (through the optional readinessChecker
// interface) and answers 503 rather than silently treating "haven't
// scanned yet" the same as "every key was revoked".
type KVIAM struct {
	keys    atomic.Pointer[map[string]KeyConfig] // hash hex -> row
	aliases atomic.Pointer[map[string]string]    // "<scope>/<name>" -> target
	ready   chan struct{}
}

// NewKVIAM starts the KEYS and ALIASES watch loops in the background and
// returns immediately (review ruling I6) — it does not wait for either
// bucket to exist or for any scan to complete. Call Ready() to find out
// when KVIAM has something real to serve. The only case NewKVIAM itself
// returns a non-nil error is ctx already being done at call time.
func NewKVIAM(ctx context.Context, js jetstream.JetStream) (*KVIAM, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	k := &KVIAM{ready: make(chan struct{})}
	emptyKeys := map[string]KeyConfig{}
	emptyAliases := map[string]string{}
	k.keys.Store(&emptyKeys)
	k.aliases.Store(&emptyAliases)

	keysReady := make(chan struct{})
	aliasesReady := make(chan struct{})

	go watchKV(ctx, js, cpkv.BucketKeys, keysReady, func(entries map[string]jetstream.KeyValueEntry) {
		m := make(map[string]KeyConfig, len(entries))
		for hash, e := range entries {
			var row cpkv.KeyEntry
			if err := json.Unmarshal(e.Value(), &row); err != nil {
				slog.Error("gateway: kviam: decode KEYS entry", "hash", truncateHash(hash), "err", err)
				continue
			}
			m[hash] = KeyConfig{ID: row.Id, Name: row.Name, Org: row.Org, Project: row.Project, Allow: row.Allow}
		}
		k.keys.Store(&m)
		slog.Info("gateway: kviam: IAM snapshot applied", "bucket", cpkv.BucketKeys, "entries", len(m))
	})

	go watchKV(ctx, js, cpkv.BucketAliases, aliasesReady, func(entries map[string]jetstream.KeyValueEntry) {
		m := make(map[string]string, len(entries))
		for key, e := range entries {
			var row cpkv.AliasEntry
			if err := json.Unmarshal(e.Value(), &row); err != nil {
				slog.Error("gateway: kviam: decode ALIASES entry", "key", key, "err", err)
				continue
			}
			m[key] = row.Target
		}
		k.aliases.Store(&m)
		slog.Info("gateway: kviam: IAM snapshot applied", "bucket", cpkv.BucketAliases, "entries", len(m))
	})

	go func() {
		select {
		case <-keysReady:
		case <-ctx.Done():
			return
		}
		select {
		case <-aliasesReady:
		case <-ctx.Done():
			return
		}
		close(k.ready)
	}()

	return k, nil
}

// Ready returns a channel that closes once both the KEYS and ALIASES
// buckets have completed their first snapshot. It never closes if ctx is
// canceled first.
func (k *KVIAM) Ready() <-chan struct{} { return k.ready }

// AuthenticateKey hashes presented with cpkv.HashKey (the same hash the
// control plane's apikey aggregate persists and the KEYS projector keys
// its bucket by) and looks the result up in the current KEYS snapshot. The
// hash computation itself is not constant-time-sensitive (binding
// constraint: "the hash lookup itself is fine") — SHA-256 of
// attacker-controlled input followed by a map lookup leaks nothing about
// any stored key that a network attacker could exploit; there is no secret
// comparison happening bit-by-bit here the way the static mode's
// ConstantTimeCompare guards against.
func (k *KVIAM) AuthenticateKey(presented string) (KeyConfig, bool) {
	hash := cpkv.HashKey(presented)
	m := *k.keys.Load()
	row, ok := m[hash]
	return row, ok
}

// ResolveAlias tries the org-scoped alias first ("<org>/<alias>"), falling
// back to the global scope ("_global/<alias>", cpkv.GlobalScope) — an
// org-scoped entry always overrides a same-named global one, never the
// reverse.
func (k *KVIAM) ResolveAlias(org, alias string) (string, bool) {
	m := *k.aliases.Load()
	if org != "" {
		if target, ok := m[org+"/"+alias]; ok {
			return target, true
		}
	}
	target, ok := m[cpkv.GlobalScope+"/"+alias]
	return target, ok
}

// watchKV runs bucket's scan+watch loop until ctx is done. On every
// (re)connect it does a from-scratch scan (via the watcher's own initial
// value replay, terminated by the nil "initial sync done" entry — see
// jetstream.KeyWatcher's doc comment) before signaling ready the first
// time, then keeps applying live updates into the same working set until
// either the watcher dies on its own (its Updates() channel closes —
// e.g. the bucket's stream was deleted) or the bucket-identity probe
// (kvProbeInterval) detects the stream was recreated, whichever comes
// first; either way it falls back to the top and re-resolves the bucket
// (retrying with backoff if the bucket is momentarily gone, e.g.
// mid-ResyncKV) and re-watches from scratch. ctx cancellation is the only
// way out for good.
func watchKV(ctx context.Context, js jetstream.JetStream, bucket string, ready chan struct{}, apply func(map[string]jetstream.KeyValueEntry)) {
	var closeReady sync.Once
	backoff := kvWatchMinBackoff
	attempt := 0

	for ctx.Err() == nil {
		attempt++

		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			kvLogFailure(bucket, "KeyValue", attempt, err)
			if !kvSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		created, err := kvStreamCreated(ctx, js, bucket)
		if err != nil {
			kvLogFailure(bucket, "stream info", attempt, err)
			if !kvSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		// watchCtx bounds this one connection attempt: it dies either with
		// the parent ctx (real shutdown) or when the identity probe (or the
		// watcher's own channel closing) decides to reconnect.
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watcher, err := kv.WatchAll(watchCtx)
		if err != nil {
			cancelWatch()
			kvLogFailure(bucket, "WatchAll", attempt, err)
			if !kvSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}
		slog.Info("gateway: kviam: connected", "bucket", bucket, "attempt", attempt)

		probeDone := make(chan struct{})
		go probeBucketIdentity(watchCtx, js, bucket, created, cancelWatch, probeDone)

		synced := consumeKV(watchCtx, watcher, apply, &closeReady, ready)

		cancelWatch()
		<-probeDone // avoid leaking the probe goroutine across reconnects
		_ = watcher.Stop()

		if synced {
			// I1 ruling: only reset backoff/attempt bookkeeping once this
			// connection actually reached a successful first sync — not
			// merely because WatchAll itself returned without error.
			backoff = kvWatchMinBackoff
			attempt = 0
		}
	}
}

// kvStreamCreated fetches the Created timestamp of the JetStream stream
// backing bucket's KV store. NATS KV buckets are implemented as a stream
// named "KV_<bucket>" — a destroy+recreate (ResyncKV's resetKVBucket)
// always produces a stream with a new Created time, so comparing this
// value across probes is a reliable way to detect that the bucket
// identity changed out from under a live watch (review ruling C1).
func kvStreamCreated(ctx context.Context, js jetstream.JetStream, bucket string) (time.Time, error) {
	strm, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return time.Time{}, err
	}
	info, err := strm.Info(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return info.Created, nil
}

// probeBucketIdentity polls kvStreamCreated every kvProbeInterval and
// calls cancelWatch (forcing watchKV to reconnect from scratch) the moment
// it observes either an error (the bucket's stream is gone, e.g. mid a
// destroy+recreate) or a Created timestamp different from baseline (the
// bucket was destroyed and recreated). It never interferes with a watch
// whose bucket identity hasn't changed, no matter how long the watch (or
// its initial replay) runs. It always closes done before returning, so
// its caller can wait for it to fully stop before starting the next
// connection attempt's probe.
func probeBucketIdentity(watchCtx context.Context, js jetstream.JetStream, bucket string, baseline time.Time, cancelWatch context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(kvProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-watchCtx.Done():
			return
		case <-ticker.C:
			created, err := kvStreamCreated(watchCtx, js, bucket)
			if err != nil {
				if watchCtx.Err() != nil {
					return // shutting down/reconnecting already
				}
				slog.Warn("gateway: kviam: bucket identity probe failed, forcing reconnect", "bucket", bucket, "err", err)
				cancelWatch()
				return
			}
			if !created.Equal(baseline) {
				slog.Info("gateway: kviam: bucket identity changed, reconnecting", "bucket", bucket)
				cancelWatch()
				return
			}
		}
	}
}

// consumeKV reads watcher.Updates() into working, calling apply with a
// full snapshot of working every time it changes (including once at the
// end of the initial replay, at which point ready is closed exactly once
// across KVIAM's whole lifetime). It returns whether the initial replay
// ever completed on this connection (used by watchKV, per review ruling
// I1, to decide whether this attempt "counts" as a successful sync for
// backoff-reset purposes) once ctx is done or the updates channel closes.
func consumeKV(ctx context.Context, watcher jetstream.KeyWatcher, apply func(map[string]jetstream.KeyValueEntry), closeReady *sync.Once, ready chan struct{}) bool {
	working := map[string]jetstream.KeyValueEntry{}
	synced := false
	for {
		select {
		case <-ctx.Done():
			return synced
		case entry, ok := <-watcher.Updates():
			if !ok {
				return synced
			}
			if entry == nil {
				// jetstream.KeyWatcher: "a nil entry" marks the end of the
				// initial value replay — everything from here on is a live
				// update.
				synced = true
				apply(working)
				closeReady.Do(func() { close(ready) })
				continue
			}
			switch entry.Operation() {
			case jetstream.KeyValuePut:
				working[entry.Key()] = entry
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				delete(working, entry.Key())
			}
			if synced {
				apply(working)
			}
		}
	}
}

// kvLogFailure emits a rate-limited warn log for a failed connect-phase
// operation (op is "KeyValue", "stream info", or "WatchAll") — see
// kvLogAlwaysUntil/kvLogEveryFailures's doc comment for the rate-limiting
// rule.
func kvLogFailure(bucket, op string, attempt int, err error) {
	if attempt <= kvLogAlwaysUntil || attempt%kvLogEveryFailures == 0 {
		slog.Warn("gateway: kviam: connect attempt failed", "bucket", bucket, "op", op, "attempt", attempt, "err", err)
	}
}

// kvSleepBackoff sleeps for *backoff (or returns false immediately if ctx
// is already done), then doubles *backoff up to kvWatchMaxBackoff.
func kvSleepBackoff(ctx context.Context, backoff *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
	}
	*backoff *= 2
	if *backoff > kvWatchMaxBackoff {
		*backoff = kvWatchMaxBackoff
	}
	return true
}
