package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
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
// Backend selection follows the es-lite delivery package (api-notes
// "go doc -all .../delivery" section):
//
//   - Postgres's Store.Drain is gap-safe (claims rows) and implements
//     delivery.Drainer ("the Postgres Store.Drain satisfies it"), so it
//     drives a delivery.Relay — the same wiring as es-lite's own
//     cmd/es-relayd/main.go: `delivery.NewRelay(store, publish, cfg)`.
//   - SQLite has no Drainer: "its single-writer log has a gap-free cursor,
//     so no claim is needed." Instead sqlite.Store implements
//     delivery.Checkpoints directly ("Checkpoints ... The sqlite.Store
//     implements it."), so it drives a delivery.Poller with the store
//     itself as both the Source (ReadAll) and the Checkpoints.
//
// Both Poller.Run and Relay.Run "drain until ctx is cancelled ... Returns
// ctx.Err() on cancellation", giving RunRelay clean shutdown with no
// goroutine leak for free.
func RunRelay(ctx context.Context, store es.Store, js jetstream.JetStream) error {
	pub := natsjs.NewPublisher(js, natsjs.DefaultSubject)

	if drainer, ok := store.(delivery.Drainer); ok {
		relay := delivery.NewRelay(drainer, pub.Handle, delivery.RelayConfig{})
		return relay.Run(ctx)
	}
	if cp, ok := store.(delivery.Checkpoints); ok {
		poller := delivery.NewPoller(store, cp, pub.Handle, delivery.Config{
			Subscriber: controlRelaySubscriber,
		})
		return poller.Run(ctx)
	}
	return fmt.Errorf("controlplane: store %T implements neither delivery.Drainer nor delivery.Checkpoints; RunRelay needs one", store)
}
