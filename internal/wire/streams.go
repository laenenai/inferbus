package wire

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// EnsureStreams idempotently provisions the two JetStream streams.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamInference,
		Subjects:  []string{"inference.req.>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		// Backstop for orphaned requests: a queued message whose gateway is
		// gone (crashed, or a bug in the disconnect/deadline cleanup path)
		// would otherwise sit in the work queue forever, waiting for a
		// worker that will never usefully serve it (its deadline will
		// already have passed). 30 minutes is comfortably longer than any
		// sane RequestTimeout while still bounding storage growth.
		MaxAge: 30 * time.Minute,
	})
	if err != nil {
		return err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamMetering,
		Subjects:  []string{"metering.usage.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	return err
}
