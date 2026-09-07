package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/natsjs"
)

// StreamControlEvents is the JetStream stream name the control plane's
// embedded relay publishes into.
const StreamControlEvents = "CONTROL_EVENTS"

// controlRelaySubscriber is the delivery.Config.Subscriber / checkpoint key
// for the embedded relay's SQLite Poller path. There is exactly one relay
// per control-plane process draining the whole log (not one per aggregate),
// so a single fixed subscriber name is the checkpoint key.
const controlRelaySubscriber = "controlplane-relay"

// EnsureControlStream idempotently provisions the CONTROL_EVENTS stream
// (spec §4): subjects ["evt.>"], a 2-minute dedup window, and 24h retention.
// natsjs.EnsureStream always sets file storage internally (es-lite v0.5.0
// natsjs/publisher.go: "Storage: jetstream.FileStorage"), so StreamConfig
// itself has no Storage field to set here.
func EnsureControlStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{
		Name:       StreamControlEvents,
		Subjects:   []string{"evt.>"},
		Duplicates: 2 * time.Minute,
		MaxAge:     24 * time.Hour,
	})
	return err
}

// RunRelay tails store's event log and republishes every event onto
// CONTROL_EVENTS through a natsjs.Publisher, blocking until ctx is done.
//
// store is deliberately typed any, not es.Store (I4 fix): the two real
// backends satisfy entirely disjoint interfaces on entirely disjoint Go
// types, and neither one is also an es.Store —
//
//   - Postgres's *postgres.Store.Drain is gap-safe (claims rows) and
//     implements delivery.Drainer ("the Postgres Store.Drain satisfies
//     it"), so it drives a delivery.Relay — the same wiring as es-lite's
//     own cmd/es-relayd/main.go: `delivery.NewRelay(store, publish, cfg)`.
//     Critically, this is the RAW, workspace-UNSCOPED *postgres.Store
//     (postgres.Open's return value): the workspace-scoped es.Store
//     returned by (*postgres.Store).Workspace(id) — the one every
//     aggregate.Runtime and projector in this package uses — implements
//     neither delivery.Drainer nor delivery.Checkpoints at all. Passing
//     that workspace-scoped store here (as this package's callers used to)
//     silently fell through to the error return below on every real
//     Postgres deployment, so the embedded relay never relayed a single
//     event: see TestRunRelay_Postgres.
//   - SQLite has no Drainer: "its single-writer log has a gap-free cursor,
//     so no claim is needed." Instead sqlite.Store implements
//     delivery.Checkpoints AND delivery.Source (ReadAll) directly
//     ("Checkpoints ... The sqlite.Store implements it."), so it drives a
//     delivery.Poller with the store itself as both.
//
// Both Poller.Run and Relay.Run "drain until ctx is cancelled ... Returns
// ctx.Err() on cancellation", giving RunRelay clean shutdown with no
// goroutine leak for free.
func RunRelay(ctx context.Context, store any, js jetstream.JetStream) error {
	pub := natsjs.NewPublisher(js, natsjs.DefaultSubject)

	if drainer, ok := store.(delivery.Drainer); ok {
		relay := delivery.NewRelay(drainer, pub.Handle, delivery.RelayConfig{})
		return relay.Run(ctx)
	}
	if cp, ok := store.(delivery.Checkpoints); ok {
		src, ok := store.(delivery.Source)
		if !ok {
			return fmt.Errorf("controlplane: store %T implements delivery.Checkpoints but not delivery.Source; RunRelay needs both for the Poller path", store)
		}
		poller := delivery.NewPoller(src, cp, pub.Handle, delivery.Config{
			Subscriber: controlRelaySubscriber,
		})
		return poller.Run(ctx)
	}
	return fmt.Errorf("controlplane: store %T implements neither delivery.Drainer nor delivery.Checkpoints; RunRelay needs one", store)
}
