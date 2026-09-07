// Package worker pulls requests off the INFERENCE work queue, runs the
// engine, streams frames back, and publishes usage events.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/wire"
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

// RunReady closes ready once every consumer is pulling.
func (w *Worker) RunReady(ctx context.Context, ready chan<- struct{}) error {
	if err := wire.EnsureStreams(ctx, w.js); err != nil {
		return err
	}
	var wg sync.WaitGroup
	var consumers []jetstream.ConsumeContext
	for _, mc := range w.cfg.Models {
		cons, err := w.js.CreateOrUpdateConsumer(ctx, wire.StreamInference, jetstream.ConsumerConfig{
			Durable:       wire.Durable(mc.Name),
			FilterSubject: wire.ReqSubject(mc.Name),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    2,
			MaxAckPending: mc.MaxInflight,
		})
		if err != nil {
			return err
		}
		mc := mc
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			// wg.Add happens synchronously on the consumer's single delivery
			// goroutine, BEFORE the handler goroutine is spawned, so it can
			// never race with the wg.Wait() below (which only runs after we
			// have confirmed, via cc.Closed(), that this delivery goroutine
			// has exited and can no longer call Add). Actual per-model
			// concurrency is bounded by MaxAckPending == mc.MaxInflight:
			// JetStream will not push more than that many un-acked messages,
			// and each handler acks only when it is fully done.
			wg.Add(1)
			go func(msg jetstream.Msg) {
				defer wg.Done()
				w.handle(ctx, mc, msg)
			}(msg)
		})
		if err != nil {
			return err
		}
		consumers = append(consumers, cc)
	}
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
		w.meter(m, mc, usage, "ok", "", false, ttft, dur, queueMS)
	case context.Cause(hctx) == canceled:
		_ = msg.Ack() // canceled requests must never redeliver
		w.meter(m, mc, usage, "canceled", "", true, ttft, dur, queueMS)
	default:
		we := wire.WireError{Code: "worker_error", Message: err.Error(), HTTPStatus: 502}
		var ee *ibengine.Error
		if errors.As(err, &ee) {
			we = wire.WireError{Code: ee.Code, Message: ee.Message, HTTPStatus: ee.HTTPStatus}
		}
		publish(wire.Message{Kind: wire.KindError, Error: &we})
		_ = msg.Ack() // terminal outcome → ack (spec §7.1)
		w.meter(m, mc, usage, "error", we.Code, true, ttft, dur, queueMS)
	}
}

func (w *Worker) meter(m reqMeta, mc ModelConfig, u wire.Usage, status, errCode string, estimated bool, ttft, dur, queueMS int64) {
	ev := wire.UsageEvent{
		ReqID: m.reqID, Org: m.org, Project: m.project, KeyID: m.keyID,
		Alias: m.alias, Model: mc.Name, Provider: mc.Engine, Kind: m.kind,
		Status: status, ErrorCode: errCode, WorkerID: w.cfg.WorkerID,
		Usage: u, TTFTMillis: ttft, DurationMillis: dur, QueueMillis: queueMS,
		Estimated: estimated, TS: time.Now().UTC(),
	}
	b, _ := json.Marshal(ev)
	if _, err := w.js.Publish(context.Background(), wire.UsageSubject(m.org, m.project, mc.Name), b); err != nil {
		slog.Error("publish usage", "req", m.reqID, "err", err)
	}
}
