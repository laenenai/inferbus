// KV projectors: ALIASES and KEYS.
//
// Both projectors are event-sourced read models (ADR 0005): each rebuilds
// its bucket by replaying the authoritative DB log (projection.Replay
// against the es.Store, never JetStream retention), then hands off to a
// durable NATS consumer for live tailing. A projection.Marker (backed here
// by a small "CP_MARKERS" KV bucket, per-projection key) records the
// highest GlobalPosition already applied, giving exact cutover from replay
// to live delivery and idempotent skip-if-already-applied on at-least-once
// redelivery (api-notes "go doc -all .../projection").
//
// The ALIASES bucket is self-contained: every AliasSet/AliasDeleted event
// carries everything the projector needs (scope+name come from the stream
// id via SplitAliasStreamID; the payload carries the target). The KEYS
// bucket is not: KeyAllowlistChanged/KeyLimitsChanged do not carry the
// key's hash, only KeyCreated/KeyRotated do, so the keys projector keeps a
// private in-memory key-id -> current-hash map, populated by Created and
// Rotated events. Because that map has no persisted form, RunKVProjectors
// always warms it up with a full, write-free replay of the whole apikey
// history (position 0) before doing the marker-scoped catch-up that
// actually mutates the KEYS bucket — otherwise a restart could lose the
// map entry for a key whose Created/Rotated event lies behind the marker,
// and a late allowlist/limits change for it would have nowhere to write.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/cpkv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/projection"
)

// BucketAliases and BucketKeys re-export cpkv's bucket names (I5 ruling:
// the schema lives in internal/cpkv now, shared with the gateway; this
// package keeps its own exported names as aliases so no existing caller
// or test needs to change).
const (
	BucketAliases = cpkv.BucketAliases
	BucketKeys    = cpkv.BucketKeys

	// bucketMarkers holds every projector's Marker position, keyed by
	// projection name. Using a KV bucket (rather than Postgres) keeps the
	// projector path store-agnostic and identical in tests (per task
	// brief).
	bucketMarkers = "CP_MARKERS"

	// durableProjAliases and durableProjKeys are both the natsjs.Consume
	// durable consumer name AND the Marker's per-projection key.
	durableProjAliases = "proj-kv-aliases"
	durableProjKeys    = "proj-kv-keys"

	// filterSubjectAliases and filterSubjectKeys narrow the live durable
	// consumers. Binding controller ruling (task brief): subjects carry the
	// WORKSPACE token (sqlite tests publish "evt.default.<agg>.<event>",
	// postgres publishes "evt.controlplane.<...>"), so the workspace
	// segment is wildcarded rather than pinned to "controlplane".
	filterSubjectAliases = "evt.*.alias.>"
	filterSubjectKeys    = "evt.*.apikey.>"

	// replayBatchSize is the page size projection.Replay reads per call.
	replayBatchSize = 100
)

// AliasEntry and KeyEntry re-export cpkv's value schemas (I5 ruling — see
// BucketAliases/BucketKeys above for why).
type (
	AliasEntry = cpkv.AliasEntry
	KeyEntry   = cpkv.KeyEntry
)

// Marker persists a read model's highest-applied GlobalPosition. Its shape
// matches projection.Marker (api-notes) exactly, so any controlplane.Marker
// also satisfies projection.Marker structurally.
type Marker interface {
	Load(ctx context.Context, name string) (uint64, error)
	Save(ctx context.Context, name string, pos uint64) error
}

// KVMarker implements Marker over a jetstream.KeyValue bucket: the
// projection name is the KV key, the value is the decimal-encoded
// position.
type KVMarker struct {
	kv jetstream.KeyValue
}

// NewKVMarker wraps kv as a Marker. kv is normally the CP_MARKERS bucket
// (see RunKVProjectors/ResyncKV), but any KeyValue works.
func NewKVMarker(kv jetstream.KeyValue) *KVMarker {
	return &KVMarker{kv: kv}
}

func (m *KVMarker) Load(ctx context.Context, name string) (uint64, error) {
	entry, err := m.kv.Get(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("controlplane: marker %q: get: %w", name, err)
	}
	pos, err := strconv.ParseUint(string(entry.Value()), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("controlplane: marker %q: parse position %q: %w", name, entry.Value(), err)
	}
	return pos, nil
}

func (m *KVMarker) Save(ctx context.Context, name string, pos uint64) error {
	if _, err := m.kv.PutString(ctx, name, strconv.FormatUint(pos, 10)); err != nil {
		return fmt.Errorf("controlplane: marker %q: save: %w", name, err)
	}
	return nil
}

// SplitAliasStreamID recovers the (scope, name) pair an alias's KV key is
// built from, out of the alias aggregate's es.StreamID instance id.
//
// AliasSet/AliasDeleted events carry no scope/name fields — only the
// stream id does — and the stream id must itself be es-lite slug-legal
// (^[a-z0-9][a-z0-9_-]*$, api-notes StreamID.Validate), which rules out a
// literal "<scope>/<name>" or a bare "_global_<name>" encoding (the slug
// regex forbids a leading '_'). The controller ruling actually adopted:
//
//   - global scope:  stream id "g_<name>"          -> scope "_global"
//   - org scope:     stream id "o_<orgid>_<name>"  -> scope "<orgid>"
//
// The leading "g"/"o" token disambiguates the two shapes; it is safe
// because orgid and name are always wire.Slug-safe and thus never contain
// "_" themselves, so splitting on the first ("g"/"o" prefix) and, for "o",
// second underscore is unambiguous.
func SplitAliasStreamID(id string) (scope, name string, err error) {
	prefix, rest, ok := strings.Cut(id, "_")
	if !ok {
		return "", "", fmt.Errorf("controlplane: alias stream id %q: missing scope prefix separator", id)
	}

	switch prefix {
	case "g":
		if rest == "" {
			return "", "", fmt.Errorf("controlplane: alias stream id %q: empty name", id)
		}
		return "_global", rest, nil

	case "o":
		orgID, name, ok := strings.Cut(rest, "_")
		if !ok {
			return "", "", fmt.Errorf("controlplane: alias stream id %q: missing org/name separator", id)
		}
		if orgID == "" || name == "" {
			return "", "", fmt.Errorf("controlplane: alias stream id %q: empty org id or name", id)
		}
		return orgID, name, nil

	default:
		return "", "", fmt.Errorf("controlplane: alias stream id %q: unknown scope prefix %q", id, prefix)
	}
}

// aliasProjector holds the ALIASES bucket's live state.
type aliasProjector struct {
	kv     jetstream.KeyValue
	marker Marker

	mu     sync.Mutex
	pos    uint64
	failed error // sticky: once set, every future apply() call is a no-op that returns it
}

func newAliasProjector(kv jetstream.KeyValue, marker Marker) *aliasProjector {
	return &aliasProjector{kv: kv, marker: marker}
}

// err returns the sticky fail-stop error, if any (see apply).
func (p *aliasProjector) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failed
}

// apply is a projection.Apply: it is used verbatim both for the replay
// catch-up and (wrapped per-envelope) for the live natsjs consumer, so the
// two paths can never drift (api-notes projection.Apply doc).
//
// Fail-stop (controller ruling, C1): applyOne/KV errors are not simply
// Nak'd-and-skipped. Live delivery calls apply once per envelope, so
// naively advancing pos/marker on the NEXT envelope's independent,
// successful apply() call would leave the earlier failure's redelivery
// permanently skipped by the "GlobalPosition <= pos" idempotency check —
// a silent, unrecoverable hole. Instead the first error is latched into
// p.failed under the same mutex that guards pos: every subsequent call
// (whether it's the same envelope redelivered or any later one) sees
// p.failed != nil and returns immediately, before ever touching pos or
// the marker. This makes it impossible for any envelope after a failure
// to advance the marker past it, regardless of delivery order or
// concurrency. The caller (RunKVProjectors) is responsible for actually
// halting delivery (cancelling the shared run context) once it observes
// this error, so a failure doesn't degenerate into an unbounded NAK loop.
func (p *aliasProjector) apply(ctx context.Context, batch []es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failed != nil {
		return p.failed
	}

	for _, e := range batch {
		if e.GlobalPosition <= p.pos {
			continue // idempotent skip: already applied
		}
		if e.StreamID.Type == alias.StreamType {
			if err := p.applyOne(ctx, e); err != nil {
				p.failed = err
				return err
			}
		}
		p.pos = e.GlobalPosition
		if err := p.marker.Save(ctx, durableProjAliases, p.pos); err != nil {
			p.failed = err
			return err
		}
	}
	return nil
}

func (p *aliasProjector) applyOne(ctx context.Context, e es.Envelope) error {
	scope, name, err := SplitAliasStreamID(e.StreamID.ID)
	if err != nil {
		return fmt.Errorf("controlplane: aliases projector: %w", err)
	}
	key := scope + "/" + name

	evt, err := alias.Codec().Decode(es.EncodedEvent{
		TypeURL:       e.TypeURL,
		SchemaVersion: e.SchemaVersion,
		Payload:       e.Payload,
	})
	if err != nil {
		return fmt.Errorf("controlplane: aliases projector: decode %s: %w", e.TypeURL, err)
	}

	switch k := evt.GetKind().(type) {
	case *controlplanev1.AliasEvent_Set:
		b, err := json.Marshal(AliasEntry{
			Target: k.Set.GetTarget(),
			Params: k.Set.GetParams(),
		})
		if err != nil {
			return err
		}
		if _, err := p.kv.Put(ctx, key, b); err != nil {
			return fmt.Errorf("controlplane: aliases projector: put %q: %w", key, err)
		}

	case *controlplanev1.AliasEvent_Deleted:
		if err := p.kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("controlplane: aliases projector: delete %q: %w", key, err)
		}
	}
	return nil
}

// keysProjector holds the KEYS bucket's live state, including the private
// key-id -> current-hash map described in the package doc comment.
type keysProjector struct {
	kv     jetstream.KeyValue
	marker Marker

	mu       sync.Mutex
	pos      uint64
	hashByID map[string]string
	failed   error // sticky: once set, every future apply() call is a no-op that returns it
}

func newKeysProjector(kv jetstream.KeyValue, marker Marker) *keysProjector {
	return &keysProjector{kv: kv, marker: marker, hashByID: map[string]string{}}
}

// err returns the sticky fail-stop error, if any (see apply).
func (p *keysProjector) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failed
}

// warmup rebuilds hashByID from the full apikey event history without
// touching the KEYS bucket or the marker — see the package doc comment for
// why this full, unmarked replay is necessary on every start.
func (p *keysProjector) warmup(ctx context.Context, store es.Store) error {
	_, err := projection.Replay(ctx, store, 0, replayBatchSize, func(ctx context.Context, batch []es.Envelope) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, e := range batch {
			if e.StreamID.Type != apikey.StreamType {
				continue
			}
			evt, err := apikey.Codec().Decode(es.EncodedEvent{
				TypeURL:       e.TypeURL,
				SchemaVersion: e.SchemaVersion,
				Payload:       e.Payload,
			})
			if err != nil {
				return fmt.Errorf("controlplane: keys projector warmup: decode %s: %w", e.TypeURL, err)
			}
			switch k := evt.GetKind().(type) {
			case *controlplanev1.ApiKeyEvent_Created:
				p.hashByID[e.StreamID.ID] = k.Created.GetHash()
			case *controlplanev1.ApiKeyEvent_Rotated:
				p.hashByID[e.StreamID.ID] = k.Rotated.GetNewHash()
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("controlplane: keys projector: warmup replay: %w", err)
	}
	return nil
}

// apply is a projection.Apply, reused verbatim for replay and (wrapped
// per-envelope) live consumption, exactly like aliasProjector.apply.
// Fail-stop semantics are identical to aliasProjector.apply (see its doc
// comment for the full rationale, C1): a sticky p.failed latch, checked
// and set under the same mutex that guards pos/hashByID, guarantees no
// envelope after a failure can advance the marker past it.
func (p *keysProjector) apply(ctx context.Context, batch []es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failed != nil {
		return p.failed
	}

	for _, e := range batch {
		if e.GlobalPosition <= p.pos {
			continue // idempotent skip: already applied
		}
		if e.StreamID.Type == apikey.StreamType {
			if err := p.applyOne(ctx, e); err != nil {
				p.failed = err
				return err
			}
		}
		p.pos = e.GlobalPosition
		if err := p.marker.Save(ctx, durableProjKeys, p.pos); err != nil {
			p.failed = err
			return err
		}
	}
	return nil
}

func (p *keysProjector) applyOne(ctx context.Context, e es.Envelope) error {
	evt, err := apikey.Codec().Decode(es.EncodedEvent{
		TypeURL:       e.TypeURL,
		SchemaVersion: e.SchemaVersion,
		Payload:       e.Payload,
	})
	if err != nil {
		return fmt.Errorf("controlplane: keys projector: decode %s: %w", e.TypeURL, err)
	}

	id := e.StreamID.ID

	switch k := evt.GetKind().(type) {
	case *controlplanev1.ApiKeyEvent_Created:
		c := k.Created
		// hashByID is only mutated AFTER the KV write succeeds (C2
		// ruling): if put fails, apply() latches p.failed and no later
		// event will ever consult this id's map entry, so there is no
		// benefit to setting it early and every reason not to — an
		// early set would let a subsequent (never-reached, but
		// hypothetically re-run) allowlist change believe a hash exists
		// in KEYS that was never actually written.
		if err := p.put(ctx, c.GetHash(), KeyEntry{
			Org:          c.GetOrg(),
			Project:      c.GetProject(),
			Name:         c.GetName(),
			Allow:        c.GetAllow(),
			RateLimitRPM: int(c.GetRateLimitRpm()),
		}); err != nil {
			return err
		}
		p.hashByID[id] = c.GetHash()
		return nil

	case *controlplanev1.ApiKeyEvent_Rotated:
		r := k.Rotated
		entry, ok, err := p.get(ctx, r.GetPreviousHash())
		if err != nil {
			return err
		}
		if !ok {
			// Defensive: Created always precedes Rotated for a valid
			// aggregate stream, so this should be unreachable, but a
			// missing previous entry must never fail the projector —
			// synthesize a minimal one rather than losing the key.
			entry = KeyEntry{}
		}
		if err := p.put(ctx, r.GetNewHash(), entry); err != nil {
			return err
		}
		if err := p.deleteEntry(ctx, r.GetPreviousHash()); err != nil {
			return err
		}
		// See the Created arm above: only mutate hashByID once every KV
		// write for this event has succeeded.
		p.hashByID[id] = r.GetNewHash()
		return nil

	case *controlplanev1.ApiKeyEvent_Disabled:
		return p.deleteEntry(ctx, k.Disabled.GetCurrentHash())

	case *controlplanev1.ApiKeyEvent_AllowlistChanged:
		hash, ok := p.hashByID[id]
		if !ok {
			return fmt.Errorf("controlplane: keys projector: allowlist change for unknown key id %q", id)
		}
		entry, ok, err := p.get(ctx, hash)
		if err != nil {
			return err
		}
		if !ok {
			// C2 ruling: a hash the map believes is current but that has
			// no KEYS entry is an invariant violation (every hash in
			// hashByID was put there by a successful Created/Rotated KV
			// write), not a normal case to paper over. Silently
			// synthesizing an entry here would Put a credential-shaped
			// KeyEntry with empty Org/Project/Name — a phantom entry —
			// under a real key's hash. Fail-stop instead.
			return fmt.Errorf("controlplane: keys projector: allowlist change for key id %q: no KEYS entry under hash %q (invariant violation)", id, hash)
		}
		entry.Allow = k.AllowlistChanged.GetAllow()
		return p.put(ctx, hash, entry)

	case *controlplanev1.ApiKeyEvent_LimitsChanged:
		// MonthlyTokenBudget is intentionally not projected into KeyEntry
		// (brief's schema has no such field) — budget enforcement is an
		// M4 concern with its own read model, not this one.
		hash, ok := p.hashByID[id]
		if !ok {
			return fmt.Errorf("controlplane: keys projector: limits change for unknown key id %q", id)
		}
		entry, ok, err := p.get(ctx, hash)
		if err != nil {
			return err
		}
		if !ok {
			// See the AllowlistChanged arm above: fail-stop rather than
			// Put a phantom entry.
			return fmt.Errorf("controlplane: keys projector: limits change for key id %q: no KEYS entry under hash %q (invariant violation)", id, hash)
		}
		entry.RateLimitRPM = int(k.LimitsChanged.GetRateLimitRpm())
		return p.put(ctx, hash, entry)
	}
	return nil
}

func (p *keysProjector) get(ctx context.Context, hash string) (KeyEntry, bool, error) {
	entry, err := p.kv.Get(ctx, hash)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return KeyEntry{}, false, nil
		}
		return KeyEntry{}, false, fmt.Errorf("controlplane: keys projector: get %q: %w", hash, err)
	}
	var v KeyEntry
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		return KeyEntry{}, false, fmt.Errorf("controlplane: keys projector: unmarshal %q: %w", hash, err)
	}
	return v, true, nil
}

func (p *keysProjector) put(ctx context.Context, hash string, entry KeyEntry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := p.kv.Put(ctx, hash, b); err != nil {
		return fmt.Errorf("controlplane: keys projector: put %q: %w", hash, err)
	}
	return nil
}

func (p *keysProjector) deleteEntry(ctx context.Context, hash string) error {
	if hash == "" {
		return nil
	}
	if err := p.kv.Delete(ctx, hash); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("controlplane: keys projector: delete %q: %w", hash, err)
	}
	return nil
}

// kvConfig returns the KeyValueConfig for one of this file's buckets
// (ALIASES, KEYS, CP_MARKERS), pinned explicitly rather than left at bare
// {Bucket: name} defaults (I5 ruling):
//
//   - History: 1 — these are current-state projections, not audit trails;
//     the event log is already the durable history, so keeping only the
//     latest KV revision per key is correct and avoids unbounded growth.
//   - Storage: jetstream.FileStorage — durable across a NATS server
//     restart, matching natsjs.EnsureStream's own choice for
//     CONTROL_EVENTS (api-notes: EnsureStream "always sets file storage
//     internally").
//
// Replicas is deliberately left at its default (1): M3 is a single-node
// JetStream topology. Revisit this (Replicas: 3 or similar) when the
// control plane is clustered.
func kvConfig(name string) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:  name,
		History: 1,
		Storage: jetstream.FileStorage,
	}
}

// ensureKVBucket idempotently creates (or adopts an existing) KV bucket.
func ensureKVBucket(ctx context.Context, js jetstream.JetStream, name string) (jetstream.KeyValue, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, kvConfig(name))
	if err != nil {
		return nil, fmt.Errorf("controlplane: ensure KV bucket %q: %w", name, err)
	}
	return kv, nil
}

// resetKVBucket destroys and recreates a KV bucket, guaranteeing an empty
// bucket regardless of prior content (used by ResyncKV).
func resetKVBucket(ctx context.Context, js jetstream.JetStream, name string) (jetstream.KeyValue, error) {
	if err := js.DeleteKeyValue(ctx, name); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("controlplane: reset KV bucket %q: delete: %w", name, err)
	}
	return ensureKVBucket(ctx, js, name)
}

// RunKVProjectors creates the ALIASES/KEYS/CP_MARKERS buckets if needed,
// replays each projector from its marker straight off store's log
// (projection.Replay — the authoritative rebuild source, not JetStream
// retention), then live-consumes durables "proj-kv-aliases"
// (evt.*.alias.>) and "proj-kv-keys" (evt.*.apikey.>) off the
// CONTROL_EVENTS stream until ctx is done.
//
// The keys projector additionally does a write-free warmup replay of the
// whole apikey history first, to rebuild its private key-id -> hash map
// (see the package doc comment).
func RunKVProjectors(ctx context.Context, store es.Store, js jetstream.JetStream) error {
	aliasesKV, err := ensureKVBucket(ctx, js, BucketAliases)
	if err != nil {
		return err
	}
	keysKV, err := ensureKVBucket(ctx, js, BucketKeys)
	if err != nil {
		return err
	}
	markersKV, err := ensureKVBucket(ctx, js, bucketMarkers)
	if err != nil {
		return err
	}
	marker := NewKVMarker(markersKV)

	aliasP := newAliasProjector(aliasesKV, marker)
	keysP := newKeysProjector(keysKV, marker)

	aliasP.pos, err = marker.Load(ctx, durableProjAliases)
	if err != nil {
		return err
	}
	keysP.pos, err = marker.Load(ctx, durableProjKeys)
	if err != nil {
		return err
	}

	if err := keysP.warmup(ctx, store); err != nil {
		return err
	}

	if _, err := projection.Replay(ctx, store, aliasP.pos, replayBatchSize, aliasP.apply); err != nil {
		return fmt.Errorf("controlplane: replay aliases projector: %w", err)
	}
	if _, err := projection.Replay(ctx, store, keysP.pos, replayBatchSize, keysP.apply); err != nil {
		return fmt.Errorf("controlplane: replay keys projector: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct{ err error }
	results := make(chan result, 2)

	// consume wraps a projector's apply as a natsjs.ProjectionHandler.
	// natsjs.Consume itself never surfaces a handler error (it just Naks
	// the message and keeps pulling — see api-notes natsjs.Consume:
	// "Returning an error naks it for redelivery"), so a handler error
	// alone would neither stop delivery nor reach RunKVProjectors's
	// return value. Fail-stop (C1 ruling) requires both: cancelling
	// runCtx here makes natsjs.Consume's "<-ctx.Done(); return ctx.Err()"
	// return promptly and stop pulling (defeating the hot-NAK-loop, I2),
	// and apply()'s sticky p.failed (checked via projErr after both
	// goroutines finish) is what RunKVProjectors actually returns —
	// ctx.Err() alone would only ever be context.Canceled, not the real
	// cause.
	consume := func(cfg natsjs.ConsumerConfig, apply func(ctx context.Context, e es.Envelope) error) {
		err := natsjs.Consume(runCtx, js, cfg, func(ctx context.Context, e es.Envelope) error {
			if err := apply(ctx, e); err != nil {
				cancel()
				return err
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			cancel() // an infra-level Consume failure stops the other consumer too
		}
		results <- result{err: err}
	}

	go consume(natsjs.ConsumerConfig{
		Stream:        StreamControlEvents,
		Durable:       durableProjAliases,
		FilterSubject: filterSubjectAliases,
	}, func(ctx context.Context, e es.Envelope) error {
		return aliasP.apply(ctx, []es.Envelope{e})
	})

	go consume(natsjs.ConsumerConfig{
		Stream:        StreamControlEvents,
		Durable:       durableProjKeys,
		FilterSubject: filterSubjectKeys,
	}, func(ctx context.Context, e es.Envelope) error {
		return keysP.apply(ctx, []es.Envelope{e})
	})

	var firstErr error
	for i := 0; i < 2; i++ {
		if r := <-results; r.err != nil && !errors.Is(r.err, context.Canceled) && firstErr == nil {
			firstErr = r.err
		}
	}
	// A projector's own fail-stop error (set by apply, under its mutex)
	// takes priority: it is the actual cause, whereas firstErr here would
	// only ever be an infra-level natsjs.Consume setup failure or that
	// same apply error surfacing a second time through Consume's return.
	if err := aliasP.err(); err != nil {
		return err
	}
	if err := keysP.err(); err != nil {
		return err
	}
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// ResyncKV purges the ALIASES and KEYS buckets (destroy+recreate, so any
// hand-edited or stale content is gone unconditionally), resets both
// projectors' markers to 0, and replays each straight from the start of
// store's log. It does not start live consumption; call RunKVProjectors
// afterward for that.
//
// Concurrency contract (I4 ruling) — this is an offline maintenance
// operation, not something safe to run alongside a live projector:
//
//   - The caller MUST stop any running RunKVProjectors (cancel its ctx and
//     wait for it to return) before calling ResyncKV. A live durable
//     consumer racing this function's bucket delete+recreate is not a
//     scenario the projector needs to tolerate, and the two would fight
//     over the same marker/KV state.
//   - While ResyncKV is running, the ALIASES/KEYS buckets do not reflect
//     a consistent snapshot: gateway request-time auth/alias lookups
//     against them are effectively unavailable (empty or partial) until
//     it completes.
//   - Bucket recreation (DeleteKeyValue + CreateKeyValue) tears down the
//     underlying JetStream stream, which kills any KV watcher subscribed
//     to it. Task 11's watcher-based cache is expected to notice its
//     watch died and re-scan the bucket from scratch rather than assume
//     watches survive a resync.
func ResyncKV(ctx context.Context, store es.Store, js jetstream.JetStream) error {
	aliasesKV, err := resetKVBucket(ctx, js, BucketAliases)
	if err != nil {
		return err
	}
	keysKV, err := resetKVBucket(ctx, js, BucketKeys)
	if err != nil {
		return err
	}
	markersKV, err := ensureKVBucket(ctx, js, bucketMarkers)
	if err != nil {
		return err
	}
	marker := NewKVMarker(markersKV)

	if err := marker.Save(ctx, durableProjAliases, 0); err != nil {
		return err
	}
	if err := marker.Save(ctx, durableProjKeys, 0); err != nil {
		return err
	}

	aliasP := newAliasProjector(aliasesKV, marker)
	keysP := newKeysProjector(keysKV, marker)

	if err := keysP.warmup(ctx, store); err != nil {
		return err
	}

	if _, err := projection.Replay(ctx, store, 0, replayBatchSize, aliasP.apply); err != nil {
		return fmt.Errorf("controlplane: resync aliases projector: %w", err)
	}
	if _, err := projection.Replay(ctx, store, 0, replayBatchSize, keysP.apply); err != nil {
		return fmt.Errorf("controlplane: resync keys projector: %w", err)
	}
	return nil
}
