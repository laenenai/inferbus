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
