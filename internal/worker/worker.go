// Package worker pulls requests off the INFERENCE work queue, runs the
// engine, streams frames back, and publishes usage events.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/wire"
)

type Worker struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	engines map[string]ibengine.Engine
	cfg     Config
}

func New(nc *nats.Conn, js jetstream.JetStream, engines map[string]ibengine.Engine, cfg Config) *Worker {
	return &Worker{nc: nc, js: js, engines: engines, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) error { return w.RunReady(ctx, nil) }

// RunReady closes ready once every consumer is pulling. ready is only
// closed on a fully successful startup: if any consumer fails to start,
// RunReady stops every consumer that did start (so nothing leaks) and
// returns the error without ever closing ready — callers must not rely on
// ready alone and should observe the returned error too.
func (w *Worker) RunReady(ctx context.Context, ready chan<- struct{}) error {
	if err := wire.EnsureStreams(ctx, w.js); err != nil {
		return err
	}
	// Fail fast on a missing engine rather than panicking with a nil-map
	// lookup deep inside handle() the first time a request for that model
	// arrives.
	for _, mc := range w.cfg.Models {
		if w.engines[mc.Name] == nil {
			return fmt.Errorf("worker: no engine registered for model %q", mc.Name)
		}
	}

	var wg sync.WaitGroup
	// Sentinel: keeps the counter >= 1 for the entire time consumers may be
	// delivering messages, so wg.Wait() below can never observe a transient
	// zero and race with a concurrent wg.Add(1) from the delivery goroutine
	// (the classic "Add called concurrently with Wait" WaitGroup misuse).
	// It is only released once every consumer's Closed() channel confirms
	// its delivery goroutine has exited and can no longer call Add.
	wg.Add(1)

	var consumers []jetstream.ConsumeContext
	var startErr error
	defer func() {
		// Only fires on a startup failure (see the early returns below);
		// on a normal shutdown consumers are already stopped explicitly
		// further down, so this is a harmless no-op there (Stop is
		// idempotent).
		if startErr != nil {
			for _, cc := range consumers {
				cc.Stop()
			}
		}
	}()

	// ackWait/maxDeliver: production defaults unless a test overrides them
	// via Config.AckWait/ConsumerMaxDeliver (see config.go's doc comment
	// on those fields — zero means "use the default", exactly like
	// AdvertiseEvery/AdvertiseTTL).
	ackWait := w.cfg.AckWait
	if ackWait == 0 {
		ackWait = 30 * time.Second
	}
	maxDeliver := w.cfg.ConsumerMaxDeliver
	if maxDeliver == 0 {
		maxDeliver = 2
	}
	for _, mc := range w.cfg.Models {
		mc := mc
		cons, err := w.js.CreateOrUpdateConsumer(ctx, wire.StreamInference, jetstream.ConsumerConfig{
			Durable:       wire.Durable(mc.Name),
			FilterSubject: wire.ReqSubject(mc.Name),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       ackWait,
			MaxDeliver:    maxDeliver,
			MaxAckPending: mc.MaxInflight,
		})
		if err != nil {
			startErr = err
			return startErr
		}
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			// wg.Add happens synchronously on the consumer's single delivery
			// goroutine, BEFORE the handler goroutine is spawned. Actual
			// per-model concurrency is bounded by MaxAckPending ==
			// mc.MaxInflight: JetStream will not push more than that many
			// un-acked messages, and each handler acks only when fully done.
			wg.Add(1)
			go func(msg jetstream.Msg) {
				defer wg.Done()
				w.handle(ctx, mc, msg)
			}(msg)
		})
		if err != nil {
			startErr = err
			return startErr
		}
		consumers = append(consumers, cc)
	}
	// Fire-and-forget: RunReady does not join this goroutine on shutdown.
	// advertise() only reads immutable Config fields (never a data race)
	// and exits promptly once ctx.Done() fires, so not waiting for it
	// here is harmless — worst case is one more best-effort Put that
	// fails fast on the already-canceled ctx.
	go w.advertise(ctx)
	if ready != nil {
		close(ready)
	}
	<-ctx.Done()
	for _, cc := range consumers {
		cc.Stop()
	}
	for _, cc := range consumers {
		<-cc.Closed()
	}
	wg.Done() // release the sentinel: no consumer can Add after this point
	wg.Wait()
	return ctx.Err()
}

type reqMeta struct {
	reqID, org, project, keyID, alias, kind, reply string
	deadline                                       time.Time
}

func metaOf(msg jetstream.Msg) reqMeta {
	h := msg.Headers()
	dl, _ := time.Parse(time.RFC3339, h.Get(wire.HdrDeadline))
	return reqMeta{
		reqID: h.Get(wire.HdrReqID), org: h.Get(wire.HdrOrg),
		project: h.Get(wire.HdrProject), keyID: h.Get(wire.HdrKeyID),
		alias: h.Get(wire.HdrAlias), kind: h.Get(wire.HdrKind),
		reply: h.Get(wire.HdrReply), deadline: dl,
	}
}

func (w *Worker) handle(ctx context.Context, mc ModelConfig, msg jetstream.Msg) {
	m := metaOf(msg)
	start := time.Now()
	queueMS := int64(0)
	if md, err := msg.Metadata(); err == nil {
		queueMS = time.Since(md.Timestamp).Milliseconds()
	}

	// Phase-1 backstop: expired before pickup → ack + canceled usage, zero tokens.
	if !m.deadline.IsZero() && time.Now().After(m.deadline) {
		_ = msg.Ack()
		w.meter(m, mc, wire.Usage{}, "canceled", "", false, 0, 0, queueMS)
		return
	}

	// Per-request context: deadline + cancel subject (phase 2).
	hctx := ctx
	var cancelFns []context.CancelFunc
	if !m.deadline.IsZero() {
		var c context.CancelFunc
		hctx, c = context.WithDeadline(hctx, m.deadline)
		cancelFns = append(cancelFns, c)
	}
	hctx, stop := context.WithCancelCause(hctx)
	cancelFns = append(cancelFns, func() { stop(nil) })
	defer func() {
		for _, c := range cancelFns {
			c()
		}
	}()
	canceled := errors.New("canceled by client")
	cancelSub, err := w.nc.Subscribe(wire.CancelSubject(m.reqID), func(*nats.Msg) { stop(canceled) })
	if err == nil {
		defer func() { _ = cancelSub.Unsubscribe() }()
	}

	// Ack-progress heartbeat while generating.
	hbDone := make(chan struct{})
	defer close(hbDone)
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-hbDone:
				return
			case <-tick.C:
				_ = msg.InProgress()
			}
		}
	}()

	seq := 0
	var firstChunk time.Time
	publish := func(fm wire.Message) {
		fm.Seq = seq
		seq++
		b, _ := json.Marshal(fm)
		if err := w.nc.Publish(m.reply, b); err != nil {
			slog.Error("publish frame", "req", m.reqID, "err", err)
		}
	}

	eng := w.engines[mc.Name]

	// A panic anywhere below (almost always inside a misbehaving engine
	// implementation) must not take the whole worker process down with it
	// — one bad request should cost this one message, not every other
	// in-flight and future request on this worker. Recover it, tell the
	// client with a normal error frame, ack (so it never redelivers into
	// the same panic), and meter it like any other terminal error.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker: recovered panic in handle", "req", m.reqID, "model", mc.Name, "panic", r)
			publish(wire.Message{Kind: wire.KindError, Error: &wire.WireError{
				Code: "worker_panic", Message: fmt.Sprintf("worker panicked: %v", r), HTTPStatus: 500,
			}})
			_ = msg.Ack() // terminal outcome → ack (spec §7.1); never redeliver into the same panic
			w.meter(m, mc, wire.Usage{}, "error", "worker_panic", true, 0, time.Since(start).Milliseconds(), queueMS)
		}
	}()

	var probe struct {
		Stream *bool `json:"stream"`
	}
	_ = json.Unmarshal(msg.Data(), &probe)
	streaming := probe.Stream != nil && *probe.Stream

	if !streaming {
		resp, usage, err := eng.Chat(hctx, mc.Name, msg.Data())
		dur := time.Since(start).Milliseconds()
		switch {
		case err == nil:
			publish(wire.Message{Kind: wire.KindResult, Payload: resp, Usage: &usage})
			_ = msg.Ack()
			w.meter(m, mc, usage, "ok", "", false, 0, dur, queueMS)
		case context.Cause(hctx) == canceled:
			_ = msg.Ack() // canceled requests must never redeliver
			w.meter(m, mc, usage, "canceled", "", true, 0, dur, queueMS)
		case errors.Is(context.Cause(hctx), context.DeadlineExceeded):
			// Deadline fired before the engine returned — same treatment as
			// an explicit client cancel: no terminal frame, meter as
			// canceled.
			_ = msg.Ack()
			w.meter(m, mc, usage, "canceled", "", true, 0, dur, queueMS)
		case errors.Is(context.Cause(hctx), context.Canceled):
			// Worker Run ctx was canceled (graceful shutdown), not a client
			// cancel or a deadline — don't report this as an engine error.
			_ = msg.Ack()
			w.meter(m, mc, usage, "canceled", "worker_shutdown", true, 0, dur, queueMS)
		default:
			we := terminalError(err)
			publish(wire.Message{Kind: wire.KindError, Error: &we})
			_ = msg.Ack() // terminal outcome → ack (spec §7.1)
			w.meter(m, mc, usage, "error", we.Code, true, 0, dur, queueMS)
		}
		return
	}

	usage, err := eng.ChatStream(hctx, mc.Name, msg.Data(), func(chunk json.RawMessage) error {
		if firstChunk.IsZero() {
			firstChunk = time.Now()
		}
		publish(wire.Message{Kind: wire.KindChunk, Payload: chunk})
		return nil
	})

	ttft := int64(0)
	if !firstChunk.IsZero() {
		ttft = firstChunk.Sub(start).Milliseconds()
	}
	dur := time.Since(start).Milliseconds()

	switch {
	case err == nil:
		publish(wire.Message{Kind: wire.KindDone, Usage: &usage})
		_ = msg.Ack()
		// A successful stream that ends with all-zero usage almost always
		// means the upstream never sent a usage payload (e.g. it ignored
		// stream_options.include_usage), not that the request genuinely
		// cost zero tokens — flag it so downstream accounting doesn't
		// silently under-bill.
		w.meter(m, mc, usage, "ok", "", usage == (wire.Usage{}), ttft, dur, queueMS)
	case context.Cause(hctx) == canceled:
		_ = msg.Ack() // canceled requests must never redeliver
		w.meter(m, mc, usage, "canceled", "", true, ttft, dur, queueMS)
	case errors.Is(context.Cause(hctx), context.DeadlineExceeded):
		// Deadline fired mid-stream (phase 2) — same treatment as an
		// explicit client cancel: no terminal frame, meter as canceled.
		_ = msg.Ack()
		w.meter(m, mc, usage, "canceled", "", true, ttft, dur, queueMS)
	case errors.Is(context.Cause(hctx), context.Canceled):
		// Worker Run ctx was canceled (graceful shutdown), not a client
		// cancel or a deadline — don't report this as an engine error.
		_ = msg.Ack()
		w.meter(m, mc, usage, "canceled", "worker_shutdown", true, ttft, dur, queueMS)
	default:
		we := terminalError(err)
		publish(wire.Message{Kind: wire.KindError, Error: &we})
		_ = msg.Ack() // terminal outcome → ack (spec §7.1)
		w.meter(m, mc, usage, "error", we.Code, true, ttft, dur, queueMS)
	}
}

// terminalError maps an engine error to the wire.WireError sent in the
// terminal error frame, unwrapping *ibengine.Error for its code/status
// when present. Shared by the streaming and non-stream terminal switches.
func terminalError(err error) wire.WireError {
	we := wire.WireError{Code: "worker_error", Message: err.Error(), HTTPStatus: 502}
	var ee *ibengine.Error
	if errors.As(err, &ee) {
		we = wire.WireError{Code: ee.Code, Message: ee.Message, HTTPStatus: ee.HTTPStatus}
	}
	return we
}

func (w *Worker) meter(m reqMeta, mc ModelConfig, u wire.Usage, status, errCode string, estimated bool, ttft, dur, queueMS int64) {
	// mc.Engine is the adapter name ("openai_http", "bifrost"), not the
	// actual model provider. For a bifrost-routed model, mc.Provider (e.g.
	// "anthropic", "openai") is the real provider and takes precedence; for
	// an openai_http model there's no separate provider config, so fall
	// back to the engine name, and finally to a fixed default so the field
	// is never empty on a usage event.
	provider := mc.Provider
	if provider == "" {
		provider = mc.Engine
	}
	if provider == "" {
		provider = "openai_http"
	}
	ev := wire.UsageEvent{
		ReqID: m.reqID, Org: m.org, Project: m.project, KeyID: m.keyID,
		Alias: m.alias, Model: mc.Name, Provider: provider, Kind: m.kind,
		Status: status, ErrorCode: errCode, WorkerID: w.cfg.WorkerID,
		Usage: u, TTFTMillis: ttft, DurationMillis: dur, QueueMillis: queueMS,
		Estimated: estimated, TS: time.Now().UTC(),
	}
	b, _ := json.Marshal(ev)
	if _, err := w.js.Publish(context.Background(), wire.UsageSubject(m.org, m.project, mc.Name), b); err != nil {
		slog.Error("publish usage", "req", m.reqID, "err", err)
	}
}
