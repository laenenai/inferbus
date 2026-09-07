package testutil

import (
	"context"
	"encoding/json"
	"time"

	"github.com/laenenai/inferbus/internal/wire"
)

// FakeEngine is a deterministic in-process engine.Engine.
type FakeEngine struct {
	Chunks     []string
	FinalUsage wire.Usage
	Err        error         // if set, returned after emitting half the chunks
	Delay      time.Duration // between chunks
}

func (f *FakeEngine) Chat(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	if f.Err != nil {
		return nil, wire.Usage{}, f.Err
	}
	if f.Delay > 0 {
		select {
		case <-ctx.Done():
			return nil, wire.Usage{}, ctx.Err()
		case <-time.After(f.Delay):
		}
	}
	return json.RawMessage(`{"object":"chat.completion"}`), f.FinalUsage, nil
}

func (f *FakeEngine) ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error) {
	// If Err is set and there are no chunks to emit first, fail immediately
	// (there's no "half the chunks" to emit before the error).
	if f.Err != nil && len(f.Chunks) == 0 {
		return wire.Usage{}, f.Err
	}

	for i, c := range f.Chunks {
		// With Err set, fail partway through instead of after every chunk,
		// so tests can observe some chunks having already been relayed
		// before the terminal error (mirrors a real engine failing
		// mid-stream, not before or after it).
		if f.Err != nil && i == len(f.Chunks)/2 {
			return wire.Usage{}, f.Err
		}

		// Delay applies between chunks, not before the first one, so
		// tests observe an immediate first chunk (e.g. for TTFT
		// measurement) followed by paced delivery of the rest.
		if i > 0 && f.Delay > 0 {
			select {
			case <-ctx.Done():
				return wire.Usage{}, ctx.Err()
			case <-time.After(f.Delay):
			}
		}

		// Check for cancellation before each emit (non-blocking) so a
		// context canceled during the delay above — or with no delay
		// configured at all — is honored deterministically instead of
		// racing with the emit call.
		select {
		case <-ctx.Done():
			return wire.Usage{}, ctx.Err()
		default:
		}

		if err := emit(json.RawMessage(c)); err != nil {
			return wire.Usage{}, err
		}
	}
	return f.FinalUsage, nil
}
