# es-lite API notes (v0.5.0)

This file is the single authority for es-lite call sites used across the M3
control-plane tasks (Tasks 2-8). It is a verbatim record of `go doc` output
against the pinned `github.com/laenenai/es-lite v0.5.0` module, captured on
2026-09-07 against branch `m3-controlplane`. **If a later task's code
disagrees with what's recorded here, these notes win** — re-run the
corresponding `go doc` command to resolve any doubt rather than trusting
memory or the local es-lite checkout (which is read-only reference for the
Decider+Codec idiom only, e.g. `examples/counter`).

Pin: `go get github.com/laenenai/es-lite@v0.5.0` (see `go.mod`/`go.sum`).

---

## `go doc github.com/laenenai/es-lite/es Decider`

```
package es // import "github.com/laenenai/es-lite/es"

type Decider[S, C, E any] struct {
	Initial    func() S
	Decide     func(state S, cmd C) (events []E, constraints []ConstraintOp, err error)
	Evolve     func(state S, event E) S
	IsTerminal func(state S) bool
}
    Decider is es-lite's aggregate model — pure functions describing
    how a stream's state evolves and which events a command produces.
    Adopted from the parent framework's ADR 0003; es-lite drops the parent's
    constraint-operations return value (cross-aggregate uniqueness is out of
    scope), leaving the classic three-function shape.

    All functions MUST be pure:

      - Initial returns the zero-value state for a stream with no events.
      - Decide is the business rule: given current state and a command,
        return the events to append plus any uniqueness constraint operations
        (ADR 0008; nil when the aggregate has no cross-stream uniqueness), or an
        error to reject the command with no state change. Events and constraints
        commit atomically.
      - Evolve folds one event into state. It runs during replay to rebuild
        state from history, so it must never call time.Now, generate IDs,
        perform I/O, or depend on anything outside its two arguments.
        Determinism here is what makes time-travel exact.
      - IsTerminal (optional) reports whether the stream is closed. The
        runtime rejects further commands on terminal streams. nil means "never
        terminal".

    Type parameters: S = state, C = command sum type, E = event sum type.
    C and E are sealed interfaces (proto oneof containers); see the codec.
```

## `go doc github.com/laenenai/es-lite/es StreamID`

```
package es // import "github.com/laenenai/es-lite/es"

type StreamID struct {
	Type string // aggregate type, slug-validated (e.g. "counter")
	ID   string // instance id, slug-validated (e.g. "main")
}
    StreamID identifies one stream: an aggregate Type and an instance ID.

    es-lite deliberately drops the parent framework's mandatory tenant
    component. Multi-tenancy, when needed, is expressed as one database
    per tenant or an application-level prefix baked into the ID — not as a
    first-class store concern (docs/adr/0001).

func NewStreamID(typ, id string) (StreamID, error)
func ParseCanonical(canonical string) (StreamID, error)
func (s StreamID) Canonical() string
func (s StreamID) String() string
func (s StreamID) Validate() error
```

## `go doc github.com/laenenai/es-lite/es Meta`

```
package es // import "github.com/laenenai/es-lite/es"

type Meta struct {
	CommandID     uuid.UUID // this command's id; generated if zero
	CorrelationID uuid.UUID // originating flow; defaults to CommandID if zero
	CausationID   uuid.UUID // the cause of this command (e.g. a prior event id)
	Actor         Actor     // who issued the command
	OccurredAt    time.Time // domain time; defaults to now if zero
}
    Meta is the command-scoped audit/causality metadata a caller supplies when
    handling a command. It is copied onto every event the command produces,
    giving the log its audit trail. The zero Meta is valid: the runtime fills in
    a fresh CommandID and OccurredAt=now when they are unset.
```

## `go doc github.com/laenenai/es-lite/es Codec`

```
package es // import "github.com/laenenai/es-lite/es"

type Codec[E any] interface {
	Encode(event E) (EncodedEvent, error)
	Decode(enc EncodedEvent) (E, error)
}
    Codec encodes and decodes an aggregate's event sum type E. By convention
    it is generated from the .proto (future codegen; for now hand-written per
    aggregate) and marshals each variant as canonical proto bytes tagged with
    the variant's full type name.

    Decode returns ErrUnknownEventType when it does not recognize the TypeURL —
    the signal for a version-skew deployment.
```

## `go doc github.com/laenenai/es-lite/es EncodedEvent`

```
package es // import "github.com/laenenai/es-lite/es"

type EncodedEvent struct {
	TypeURL       string
	SchemaVersion uint32
	Payload       []byte
}
    EncodedEvent is the wire form of a single event: a TypeURL that identifies
    the variant, its schema version, and the serialized bytes. It is the
    boundary between the typed aggregate world (sealed event interfaces) and the
    byte-oriented Store.
```

## `go doc github.com/laenenai/es-lite/es Envelope`

```
package es // import "github.com/laenenai/es-lite/es"

type Envelope struct {
	// Identity & ordering
	EventID        uuid.UUID // UUIDv7 — globally unique, time-ordered
	StreamID       StreamID
	Version        uint64 // per-stream, 1-based, contiguous
	GlobalPosition uint64 // monotonic across all streams; the delivery cursor

	// Workspace is the source workspace, carried for delivery routing and
	// NATS subject construction (ADR 0003/0004). It is NOT part of stream
	// identity (StreamID stays Type:ID) — it is provenance the storage
	// adapter fills in. Empty for single-workspace backends (SQLite).
	Workspace string

	// Type & schema
	TypeURL       string // e.g. "counter.v1.Incremented"; the codec dispatch key
	SchemaVersion uint32 // event schema version (default 1); enables future upcasting

	// Time — two distinct clocks, both load-bearing for time-travel.
	OccurredAt time.Time // domain time, supplied by the command
	RecordedAt time.Time // DB commit time, set by the adapter; the as-of-time basis

	// Causality & audit
	CorrelationID uuid.UUID // groups all events from one originating request/flow
	CausationID   uuid.UUID // the event/command that directly caused this one
	CommandID     uuid.UUID // the command that produced this event
	Actor         Actor     // who caused it

	// Payload — canonical serialized event bytes (proto by convention).
	Payload []byte
}
    Envelope is the Go-side wrapper around every stored event. The Store
    persists and returns Envelopes; it treats Payload as opaque bytes tagged by
    TypeURL and never inspects it (docs/adr/0001, "storage is neutral").

    The field set is deliberately smaller than the parent framework's:
    no tenant, no crypto key-refs, no tamper-evidence hash chain. What remains
    is identity, ordering, schema, the two timestamps, and the audit/causality
    metadata that make the log a genuine audit trail.
```

## `go doc github.com/laenenai/es-lite/es Store`

```
package es // import "github.com/laenenai/es-lite/es"

type Store interface {
	// Append commits one batch of events for a single stream in one
	// transaction. It enforces optimistic concurrency against
	// p.ExpectedVersion and returns ErrConflict on mismatch. The
	// returned Envelopes are fully populated (Version, GlobalPosition,
	// RecordedAt, EventID).
	Append(ctx context.Context, p AppendParams) (AppendResult, error)

	// ReadStream returns a stream's events with version in the half-open
	// range (fromVersion, toVersion], ordered ascending. fromVersion=0
	// starts at the beginning; toVersion=0 means "no upper bound". So
	// ReadStream(sid, 0, 0) is the whole stream, and ReadStream(sid, 0, N)
	// is the prefix used for time-travel-by-version.
	ReadStream(ctx context.Context, sid StreamID, fromVersion, toVersion uint64) ([]Envelope, error)

	// ReadStreamAsOf returns a stream's events with RecordedAt <= asOf,
	// ordered ascending — the basis for time-travel-by-wall-clock.
	// RecordedAt (DB commit time) is used, not OccurredAt, because it is
	// the reproducible, monotonic clock.
	ReadStreamAsOf(ctx context.Context, sid StreamID, asOf time.Time) ([]Envelope, error)

	// ReadAll returns up to limit events with GlobalPosition >
	// fromPosition across all streams, ordered ascending. This is the
	// delivery cursor the poller tails — the log acting as the outbox.
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]Envelope, error)

	// CurrentStreamVersion returns the highest committed version for a
	// stream, or 0 if the stream has no events.
	CurrentStreamVersion(ctx context.Context, sid StreamID) (uint64, error)

	// LookupClaim returns the stream that currently holds the uniqueness
	// claim (scope, value) — the reverse of Claim. `value` and `pii` are the
	// same as passed to Claim; the store keys the value identically, so a PII
	// claim is looked up by the same keyed HMAC. `found` is false when no
	// stream holds it (never claimed, or since released).
	//
	// This exposes the uniqueness index as a read: any aggregate that Claims a
	// value gets reverse lookup (e.g. external-identity → user_id) with
	// read-your-writes consistency, since the claim commits in the append
	// transaction — no separate, eventually-consistent projection needed.
	LookupClaim(ctx context.Context, scope, value string, pii bool) (streamID string, found bool, err error)
}
    Store is the storage contract every backend implements. It is small and
    serialization-neutral: it moves Envelopes (opaque Payload + metadata) in and
    out, and knows nothing about aggregates, proto, or projections. Adapters
    live under sqlite/ (and, later, postgres/).

    The event log is append-only and immutable: there is no Update or Delete.
    History, auditability, and time-travel all rest on that invariant
    (docs/adr/0001).
```

## `go doc github.com/laenenai/es-lite/aggregate Runtime`

```
package aggregate // import "github.com/laenenai/es-lite/aggregate"

type Runtime[S, C, E any] struct {
	// Has unexported fields.
}
    Runtime handles commands for one aggregate type.

func NewRuntime[S, C, E any](store es.Store, decider es.Decider[S, C, E], codec es.Codec[E]) *Runtime[S, C, E]
func (r *Runtime[S, C, E]) Handle(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) (Result[S, E], error)
func (r *Runtime[S, C, E]) Load(ctx context.Context, sid es.StreamID) (state S, version uint64, err error)
func (r *Runtime[S, C, E]) LoadAsOfTime(ctx context.Context, sid es.StreamID, t time.Time) (S, error)
func (r *Runtime[S, C, E]) LoadAsOfVersion(ctx context.Context, sid es.StreamID, v uint64) (S, error)
func (r *Runtime[S, C, E]) LoadStale(ctx context.Context, sid es.StreamID, maxAge time.Duration) (state S, version uint64, fresh bool, err error)
func (r *Runtime[S, C, E]) WithSnapshots(cache snapshot.Cache, stateCodec es.StateCodec[S], foldVersion uint32) *Runtime[S, C, E]
func (r *Runtime[S, C, E]) WithUpcaster(u es.Upcaster[E]) *Runtime[S, C, E]
```

## `go doc github.com/laenenai/es-lite/aggregate NewRuntime`

```
package aggregate // import "github.com/laenenai/es-lite/aggregate"

func NewRuntime[S, C, E any](store es.Store, decider es.Decider[S, C, E], codec es.Codec[E]) *Runtime[S, C, E]
    NewRuntime wires a Runtime against a Store, a Decider, and the event Codec
    (generated per aggregate; hand-written for now).
```

## `go doc github.com/laenenai/es-lite/aggregate Runtime.Handle`

```
package aggregate // import "github.com/laenenai/es-lite/aggregate"

func (r *Runtime[S, C, E]) Handle(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) (Result[S, E], error)
    Handle loads the stream, applies cmd via Decider.Decide, and appends
    the resulting events. On an optimistic-concurrency clash it returns
    es.ErrConflict — the caller reloads and retries. The command's audit
    metadata is taken from meta; missing CommandID/OccurredAt are filled in here
    (the runtime, unlike the Decider, may touch the clock and generate IDs).
```

## `go doc github.com/laenenai/es-lite/sqlite Open`

```
package sqlite // import "github.com/laenenai/es-lite/sqlite"

func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error)
    Open opens (creating if needed) a SQLite database at dsn and applies
    the schema. dsn is a modernc.org/sqlite DSN, e.g. "file:events.db" or
    "file:mem?mode=memory&cache=shared". Pragmas for WAL, busy-timeout,
    and foreign keys are set automatically.
```

## `go doc github.com/laenenai/es-lite/postgres Open`

```
package postgres // import "github.com/laenenai/es-lite/postgres"

func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error)
    Open connects to Postgres and applies migrations (unless
    WithoutAutoMigrate).
```

## `go doc -all github.com/laenenai/es-lite/delivery` (whole package)

```
package delivery // import "github.com/laenenai/es-lite/delivery"

Package delivery tails the event log and hands batches to a subscriber.
It is the "log as outbox" relay from docs/adr/0001: no separate outbox table,
just a cursor over the append-only events table advanced through a durable
per-subscriber checkpoint.

Delivery is at-least-once. A crash between handling a batch and saving the
checkpoint re-delivers that batch on restart, so handlers MUST be idempotent.

TYPES

type Checkpoints interface {
	LoadCheckpoint(ctx context.Context, subscriber string) (uint64, error)
	SaveCheckpoint(ctx context.Context, subscriber string, position uint64) error
}
    Checkpoints persists each subscriber's progress. The sqlite.Store implements
    it.

type Config struct {
	// Subscriber is the checkpoint key; unique per independent consumer.
	Subscriber string

	// BatchSize caps events read per query. Default 100.
	BatchSize int

	// PollInterval is the fallback tick — the safety net that bounds
	// latency when no wake Signal fires (and the sole driver if none is
	// wired). Default 1s.
	PollInterval time.Duration

	// Wake, if set, lets a push source (Postgres LISTEN/NOTIFY, or an
	// in-process Signal on SQLite) trigger an immediate drain instead of
	// waiting for the next tick. Optional.
	Wake <-chan struct{}
}
    Config configures a Poller.

type Drainer interface {
	Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error)
}
    Drainer claims and publishes unpublished events in one gap-safe operation,
    returning the number of rows claimed. The Postgres Store.Drain satisfies it.
    (SQLite delivery uses Poller instead: its single-writer log has a gap-free
    cursor, so no claim is needed.)

type Handler func(ctx context.Context, batch []es.Envelope) error
    Handler processes one ordered batch of events. Returning an error aborts the
    current drain without advancing the checkpoint, so the batch is retried on
    the next drain (at-least-once).

type Poller struct {
	// Has unexported fields.
}
    Poller drains the log for one subscriber.

func NewPoller(src Source, cp Checkpoints, handler Handler, cfg Config) *Poller
    NewPoller wires a Poller. Panics if Subscriber or handler is empty — both
    are programmer errors, not runtime conditions.

func (p *Poller) Run(ctx context.Context) error
    Run drains until ctx is cancelled. It drains once immediately (to catch
    up on anything appended while it was down), then on each wake or tick.
    Returns ctx.Err() on cancellation.

type Relay struct {
	// Has unexported fields.
}
    Relay drives a Drainer continuously, publishing claimed batches through
    a Handler (typically the NATS Publisher.Handle). It is the Postgres-side
    counterpart to Poller: run one per subscriber, as a singleton or
    leader-elected (ADR 0002/0003).

func NewRelay(drainer Drainer, publish Handler, cfg RelayConfig) *Relay
    NewRelay wires a Relay. Panics if publish is nil.

func (r *Relay) Run(ctx context.Context) error
    Run drains until ctx is cancelled: an initial catch-up, then on each wake or
    tick. Returns ctx.Err() on cancellation.

type RelayConfig struct {
	// BatchSize caps rows claimed per Drain. Default 100.
	BatchSize int
	// PollInterval is the fallback tick when no wake fires. Default 1s.
	PollInterval time.Duration
	// Wake, if set, triggers an immediate drain (Postgres LISTEN/NOTIFY via
	// a delivery.Signal). Optional.
	Wake <-chan struct{}
}
    RelayConfig configures a Relay.

type Signal struct {
	// Has unexported fields.
}
    Signal is a coalescing wake-up primitive. Producers call Notify after
    committing events; a Poller selects on C() to drain immediately instead of
    waiting for its next fallback tick.

    It is the seam that lets Postgres LISTEN/NOTIFY (or any push source)
    eliminate polling latency: a pg listener goroutine calls Notify on each
    NOTIFY, and the same Poller code that works on SQLite reacts with no
    changes. On SQLite the application simply calls Notify itself after a
    successful append.

    Notify never blocks: if a wake is already pending it is a no-op, so N rapid
    appends collapse into at most one extra drain. Delivery correctness never
    depends on a signal arriving — a missed Notify only delays events to the
    next fallback poll; the checkpoint guarantees nothing is skipped.

func NewSignal() *Signal
    NewSignal creates a Signal with a depth-1 coalescing buffer.

func (s *Signal) C() <-chan struct{}
    C returns the channel a Poller waits on.

func (s *Signal) Notify()
    Notify requests a drain. Non-blocking and idempotent between drains.

type Source interface {
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error)
}
    Source is the read side of the log the poller tails. es.Store satisfies it.
```

## `go doc -all github.com/laenenai/es-lite/natsjs` (whole package)

```
package natsjs // import "github.com/laenenai/es-lite/natsjs"

Package natsjs is the NATS/JetStream delivery adapter for es-lite (ADR 0003).
It bridges the DB event log to a JetStream stream and drives projections as
durable consumers.

The relay side (Publisher) plugs into either backend's delivery path — the
Postgres Store.Drain or the SQLite delivery.Poller — as a delivery.Handler.
Each event is published with Nats-Msg-Id = global_position, so JetStream
deduplicates: the relay can run N-way or fail over without emitting an event
twice. The DB log stays the source of truth; JetStream is transport and
fast-replay substrate, never the archive.

This package uses the official nats.go/jetstream client directly (natskit is
core-NATS only). Connection creation is left to the caller so it can reuse the
service's existing nats.Conn / credentials.

CONSTANTS

const (
	HdrMsgID          = "Nats-Msg-Id" // JetStream dedup key = global_position
	HdrEventID        = "Es-Event-Id"
	HdrTypeURL        = "Es-Type-Url"
	HdrSchemaVersion  = "Es-Schema-Version"
	HdrStream         = "Es-Stream" // canonical "type:id"
	HdrWorkspace      = "Es-Workspace"
	HdrVersion        = "Es-Version"
	HdrGlobalPosition = "Es-Global-Position"
	HdrOccurredAt     = "Es-Occurred-At"
	HdrRecordedAt     = "Es-Recorded-At"
	HdrCorrelationID  = "Es-Correlation-Id"
	HdrCausationID    = "Es-Causation-Id"
	HdrCommandID      = "Es-Command-Id"
	HdrActorType      = "Es-Actor-Type"
	HdrActorID        = "Es-Actor-Id"
)
    Header names carried on each published message. The body is the event
    payload bytes; everything else rides in headers so consumers can route and
    filter without decoding the body.


FUNCTIONS

func Connect(url string, opts ...nats.Option) (*nats.Conn, error)
    Connect opens a NATS connection. Callers may instead bring their own
    *nats.Conn (e.g. cmd/es-lited's natsConnect helper) and pass it to
    jetstream.New.

func Consume(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig, handler ProjectionHandler) error
    Consume runs a durable consumer until ctx is cancelled, decoding each
    message to an es.Envelope and invoking handler. To rebuild a projection,
    reset/recreate the durable and replay (ADR 0005).

func DecodeEnvelope(header nats.Header, data []byte) (es.Envelope, error)
    DecodeEnvelope reconstructs an Envelope from a delivered message's headers
    and body. Projection handlers use it to get back a typed es.Envelope.

func DefaultSubject(e es.Envelope) string
    DefaultSubject builds evt.<workspace>.<aggregate>.<event>, lower-casing
    the event type and substituting "default" for an empty workspace (single-
    workspace backends). Tokens are sanitized to valid NATS subject characters.

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error)
    EnsureStream creates or updates the events stream.


TYPES

type ConsumerConfig struct {
	Stream  string // stream to bind to
	Durable string // durable name = the projection's identity (its checkpoint)

	// FilterSubject narrows what the projection sees, e.g. "evt.*.counter.>"
	// for one aggregate across workspaces, or "evt.ws_a.>" for one workspace.
	// Empty consumes the whole stream.
	FilterSubject string
}
    ConsumerConfig configures a durable projection consumer.

type ProjectionHandler func(ctx context.Context, e es.Envelope) error
    ProjectionHandler processes one delivered event. Returning nil acks it;
    returning an error naks it for redelivery. Delivery is at-least-once,
    so handlers MUST be idempotent (ADR 0003) — e.g. track the last-applied
    global_position per read model (ADR 0005).

type Publisher struct {
	// Has unexported fields.
}
    Publisher publishes envelopes to JetStream. Its Handle method is a
    delivery.Handler, so it drops straight into the Postgres Store.Drain or the
    SQLite delivery.Poller as the relay's publish step.

func NewPublisher(js jetstream.JetStream, subject SubjectFunc) *Publisher
    NewPublisher builds a Publisher. subject may be nil (uses DefaultSubject).

func (p *Publisher) Handle(ctx context.Context, batch []es.Envelope) error
    Handle publishes a batch in order, stopping at the first error so the caller
    (Drain / Poller) does not advance its checkpoint past an event that was not
    published. Signature matches delivery.Handler and Drain's publish.

func (p *Publisher) Publish(ctx context.Context, e es.Envelope) error
    Publish sends one envelope, deduplicated on global_position.

type StreamConfig struct {
	Name     string   // e.g. "ES_EVENTS"
	Subjects []string // default ["evt.>"]

	// Duplicates is the dedup window keyed on Nats-Msg-Id (= global_position).
	// It MUST exceed max relay lag + failover time, so a re-electing relay's
	// replays fall inside it (ADR 0003). Default 2m.
	Duplicates time.Duration

	// MaxAge bounds retention. Retention is a fast-replay convenience, not an
	// archive — deep rebuilds read the DB log. 0 keeps per server limits.
	MaxAge time.Duration
}
    StreamConfig configures the JetStream stream that captures es-lite events.

type SubjectFunc func(e es.Envelope) string
    SubjectFunc derives a NATS subject from an event. The
    default follows the nats-contracts lifecycle-event taxonomy:
    evt.<workspace>.<aggregate>.<event>.
```

## `go doc -all github.com/laenenai/es-lite/projection` (whole package)

```
package projection // import "github.com/laenenai/es-lite/projection"

Package projection provides the read-model rebuild primitive (ADR 0005):
replay the event log through the same handler the live consumer uses.

FUNCTIONS

func Replay(ctx context.Context, src Source, from uint64, batch int, apply Apply) (uint64, error)
    Replay pages src.ReadAll from `from` (exclusive) in global order,
    applying each batch, and returns the last global_position applied. This is
    the authoritative rebuild source — the DB log, not JetStream retention (ADR
    0005). Apply must be idempotent; pair it with a Marker so a rebuild can cut
    over to live delivery at exactly the returned position.


TYPES

type Apply func(ctx context.Context, batch []es.Envelope) error
    Apply handles one ordered batch. It has the same shape as delivery.Handler,
    so a projection's live handler and its rebuild handler are the same function
    — they cannot drift.

type Marker interface {
	Load(ctx context.Context, projection string) (uint64, error)
	Save(ctx context.Context, projection string, position uint64) error
}
    Marker persists a read model's highest-applied global_position. This single
    value does double duty (ADR 0005): live delivery skips events whose position
    is <= the marker (idempotent at-least-once), and a rebuild resumes live
    delivery at exactly the marker (exact cutover). Implementations live beside
    the read model — the sqlite/postgres checkpoints table can serve as one.

type Source interface {
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error)
}
    Source is the read side Replay pages over. es.Store and the workspace-scoped
    Postgres store both satisfy it. (For Postgres, use a workspace-scoped store
    to replay one workspace, or an admin reader for all.)
```

---

## Summary for later tasks (the 5-line version)

1. `es.Decider[S,C,E]{Initial,Decide,Evolve,IsTerminal}` is hand-written per
   aggregate; `aggregate.NewRuntime(store, decider, codec)` wires it, and
   `Runtime.Handle(ctx, es.StreamID, cmd C, es.Meta)` is the single write
   entry point (returns `es.ErrConflict` on optimistic-concurrency clash).
2. `es.Codec[E]{Encode,Decode}` is hand-written today (no codegen yet) and
   maps each event variant to `es.EncodedEvent{TypeURL, SchemaVersion, Payload}`;
   `Decode` returns `es.ErrUnknownEventType` on an unrecognized TypeURL.
3. Storage: `sqlite.Open(ctx, dsn, opts...)` / `postgres.Open(ctx, dsn, opts...)`
   both return `(*Store, error)` implementing `es.Store` (Append/ReadStream/
   ReadStreamAsOf/ReadAll/CurrentStreamVersion/LookupClaim).
4. Delivery: SQLite uses `delivery.NewPoller(src, cp, handler, cfg)` +
   `Poller.Run(ctx)`; Postgres uses `delivery.NewRelay(drainer, publish, cfg)`
   + `Relay.Run(ctx)` (`drainer.Drain` is gap-safe/claims rows). Both take a
   `delivery.Handler = func(ctx, []es.Envelope) error`; an optional
   `delivery.Signal` (`NewSignal`/`.C()`/`.Notify()`) short-circuits the poll
   tick.
5. NATS/JetStream (`natsjs`): `NewPublisher(js, subjectFn).Handle` is a
   `delivery.Handler` that fans events onto JetStream (dedup via
   `Nats-Msg-Id = global_position`); `Consume(ctx, js, ConsumerConfig, handler)`
   runs a durable projection consumer; `projection.Replay(ctx, src, from,
   batch, apply)` rebuilds a read model straight from the DB log (the
   authoritative source, not JetStream retention), paired with a
   `projection.Marker` for exact live-delivery cutover.
