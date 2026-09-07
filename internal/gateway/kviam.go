// iamProvider is the seam introduced by Task 11 between the gateway's
// request path (authenticate + alias resolution in gateway.go) and however
// key/alias authority is actually sourced. Two implementations exist:
//
//   - staticIAM wraps the pre-existing static Config (M2 behavior,
//     unchanged) — the default, and what every pre-Task-11 gateway test
//     still exercises.
//   - KVIAM sources both maps live from the control plane's ALIASES/KEYS
//     NATS KV buckets (Tasks 7/10), for `iam.mode: kv` deployments.
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

	"github.com/laenenai/inferbus/internal/controlplane"
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

// kvGlobalScope mirrors controlplane's unexported globalScope ("_global")
// and admin.go's aliasStreamID split (SplitAliasStreamID maps the "g_"
// stream-id prefix to this exact scope string) — every ALIASES KV key
// under this scope is a cross-org default. It is a plain string constant,
// not an import, because controlplane's globalScope constant itself is
// unexported.
const kvGlobalScope = "_global"

// kvWatchMinBackoff/kvWatchMaxBackoff bound the retry delay a KVIAM watch
// loop uses between a failed bucket lookup/watch attempt and the next one
// (binding ruling #1/#2: bounded exponential backoff, not a hot loop, and
// not a permanent failure). The delay resets to the minimum every time a
// watch is successfully (re)established, so a single blip doesn't leave a
// watcher waiting the maximum delay on its next disconnect.
const (
	kvWatchMinBackoff = 200 * time.Millisecond
	kvWatchMaxBackoff = 10 * time.Second
)

// kvWatchRefreshInterval bounds how long any single underlying watcher is
// trusted before watchKV proactively tears it down and reconnects from
// scratch, regardless of whether its update channel ever errored on its
// own.
//
// This exists because of an observed characteristic of the nats.go
// client's ordered-consumer KV watcher (not something KVIAM controls):
// when a bucket is destroyed and recreated out from under a live watch
// (as controlplane.ResyncKV's resetKVBucket does), the watcher's
// Updates() channel does not close promptly — a standalone probe against
// this exact client version measured anywhere from roughly 10s to 40s
// before the channel closed on its own, apparently gated by the client's
// internal consumer-heartbeat/idle detection. Relying solely on that
// passive signal would mean auth/alias state could stay stale for the
// better part of a minute after a resync completes — a materially worse
// guarantee than binding ruling #1 intends ("re-scanning and
// re-watching", not "eventually, on the client library's own schedule").
// Forcing a fresh scan+watch every kvWatchRefreshInterval instead puts a
// hard, KVIAM-owned ceiling on that staleness window, independent of the
// client's own detection timing.
const kvWatchRefreshInterval = 5 * time.Second

// KVIAM is the control-plane-backed iamProvider (Task 11): it scans and
// live-watches the KEYS and ALIASES KV buckets (controlplane.BucketKeys/
// BucketAliases) into two atomically-swapped maps, so the gateway's hot
// request path (AuthenticateKey/ResolveAlias) never itself touches NATS —
// every request reads a already-materialized snapshot.
//
// Resilience (binding ruling #1): the ALIASES/KEYS buckets are destroyed
// and recreated wholesale during an admin resync (controlplane.ResyncKV),
// which kills any KV watcher subscribed to them. KVIAM's watch loop
// notices its watcher died (the KeyValueEntry channel closes) or that a
// lookup/watch call itself failed, and responds by re-resolving the
// bucket and re-watching from scratch — not by crashing the gateway or
// serving a stale-forever map. A KEYS/ALIASES entry that no longer exists
// (a disabled key is deleted upstream, per kvproj.go) is absent from the
// map, and absence is deny — there is no separate "disabled" flag to
// check on this side.
//
// Missing bucket at startup (binding ruling #2): the control plane may
// start after the gateway (e.g. in compose). NewKVIAM does not treat a
// missing bucket as fatal; each watch loop polls with backoff until the
// bucket appears or ctx is done.
type KVIAM struct {
	keys    atomic.Pointer[map[string]KeyConfig] // hash hex -> row
	aliases atomic.Pointer[map[string]string]    // "<scope>/<name>" -> target
}

// NewKVIAM starts the KEYS and ALIASES watch loops and blocks until each
// has completed its first successful scan, or ctx is done — whichever
// comes first. Per binding ruling #2, a bucket that does not exist yet is
// retried with backoff rather than surfaced as an error; NewKVIAM only
// ever returns a non-nil error when ctx itself is canceled/expired before
// both buckets became available.
func NewKVIAM(ctx context.Context, js jetstream.JetStream) (*KVIAM, error) {
	k := &KVIAM{}
	emptyKeys := map[string]KeyConfig{}
	emptyAliases := map[string]string{}
	k.keys.Store(&emptyKeys)
	k.aliases.Store(&emptyAliases)

	keysReady := make(chan struct{})
	aliasesReady := make(chan struct{})

	go watchKV(ctx, js, controlplane.BucketKeys, keysReady, func(entries map[string]jetstream.KeyValueEntry) {
		m := make(map[string]KeyConfig, len(entries))
		for hash, e := range entries {
			var row controlplane.KeyEntry
			if err := json.Unmarshal(e.Value(), &row); err != nil {
				slog.Error("gateway: kviam: decode KEYS entry", "hash", hash, "err", err)
				continue
			}
			m[hash] = KeyConfig{Name: row.Name, Org: row.Org, Project: row.Project, Allow: row.Allow}
		}
		k.keys.Store(&m)
	})

	go watchKV(ctx, js, controlplane.BucketAliases, aliasesReady, func(entries map[string]jetstream.KeyValueEntry) {
		m := make(map[string]string, len(entries))
		for key, e := range entries {
			var row controlplane.AliasEntry
			if err := json.Unmarshal(e.Value(), &row); err != nil {
				slog.Error("gateway: kviam: decode ALIASES entry", "key", key, "err", err)
				continue
			}
			m[key] = row.Target
		}
		k.aliases.Store(&m)
	})

	select {
	case <-keysReady:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-aliasesReady:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return k, nil
}

// AuthenticateKey hashes presented with controlplane.HashKey (the same
// hash the control plane's apikey aggregate persists and the KEYS
// projector keys its bucket by — keys.go's doc comment) and looks the
// result up in the current KEYS snapshot. The hash computation itself is
// not constant-time-sensitive (binding constraint: "the hash lookup
// itself is fine") — SHA-256 of attacker-controlled input followed by a
// map lookup leaks nothing about any stored key that a network attacker
// could exploit; there is no secret comparison happening bit-by-bit here
// the way the static mode's ConstantTimeCompare guards against.
func (k *KVIAM) AuthenticateKey(presented string) (KeyConfig, bool) {
	hash := controlplane.HashKey(presented)
	m := *k.keys.Load()
	row, ok := m[hash]
	return row, ok
}

// ResolveAlias tries the org-scoped alias first ("<org>/<alias>"), falling
// back to the global scope ("_global/<alias>") — an org-scoped entry
// always overrides a same-named global one, never the reverse.
func (k *KVIAM) ResolveAlias(org, alias string) (string, bool) {
	m := *k.aliases.Load()
	if org != "" {
		if target, ok := m[org+"/"+alias]; ok {
			return target, true
		}
	}
	target, ok := m[kvGlobalScope+"/"+alias]
	return target, ok
}

// watchKV runs bucket's scan+watch loop until ctx is done. On every
// (re)connect it does a from-scratch scan (via the watcher's own initial
// value replay, terminated by the nil "initial sync done" entry — see
// jetstream.KeyWatcher's doc comment) before signaling ready the first
// time, then keeps applying live updates into the same working set until
// that watcher either dies on its own (its Updates() channel closes —
// bucket deleted/recreated out from under it, or any other terminal watch
// error) or kvWatchRefreshInterval elapses, whichever comes first; either
// way it falls back to the top and re-resolves the bucket (retrying with
// backoff if the bucket is momentarily gone, e.g. mid-ResyncKV) and
// re-watches from scratch. ctx cancellation is the only way out for good.
func watchKV(ctx context.Context, js jetstream.JetStream, bucket string, ready chan struct{}, apply func(map[string]jetstream.KeyValueEntry)) {
	var closeReady sync.Once
	backoff := kvWatchMinBackoff

	for ctx.Err() == nil {
		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			if !kvSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		// watchCtx bounds this one connection attempt: it dies either
		// with the parent ctx (real shutdown) or when the refresh timer
		// below fires (proactive reconnect), whichever happens first.
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watcher, err := kv.WatchAll(watchCtx)
		if err != nil {
			cancelWatch()
			if !kvSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		// A live watcher is up: reset the backoff so a *future*
		// disconnect starts retrying at the minimum delay again, rather
		// than inheriting whatever this attempt's backoff had climbed to.
		backoff = kvWatchMinBackoff
		refresh := time.AfterFunc(kvWatchRefreshInterval, cancelWatch)

		consumeKV(watchCtx, watcher, apply, &closeReady, ready)

		refresh.Stop()
		cancelWatch()
		_ = watcher.Stop()
		// consumeKV returns when watchCtx is done (parent ctx canceled OR
		// the refresh timer fired) or the watcher's update channel closed
		// out from under it; the loop condition above (checked against
		// the real parent ctx, not watchCtx) is what actually ends the
		// loop on shutdown — a refresh-timer firing falls straight
		// through to reconnect.
	}
}

// consumeKV reads watcher.Updates() into working, calling apply with a
// full snapshot of working every time it changes (including once at the
// end of the initial replay, at which point ready is closed exactly
// once). It returns when ctx is done or the updates channel closes.
func consumeKV(ctx context.Context, watcher jetstream.KeyWatcher, apply func(map[string]jetstream.KeyValueEntry), closeReady *sync.Once, ready chan struct{}) {
	working := map[string]jetstream.KeyValueEntry{}
	synced := false
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
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
