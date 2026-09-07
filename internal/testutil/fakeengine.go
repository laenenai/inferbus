package testutil

import (
	"context"
	"encoding/json"
	"time"

	"github.com/infbus/infbus/internal/wire"
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
	return json.RawMessage(`{"object":"chat.completion"}`), f.FinalUsage, nil
}

func (f *FakeEngine) ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error) {
	// Issue 1: Err swallowed on empty Chunks — half of zero is immediately
	if f.Err != nil && len(f.Chunks) == 0 {
		return wire.Usage{}, f.Err
	}

	for i, c := range f.Chunks {
		// Mid-loop error return for non-empty Chunks (half-way point)
		if f.Err != nil && i == len(f.Chunks)/2 {
			return wire.Usage{}, f.Err
		}

		// Issue 2: Delay applies BETWEEN chunks only (not before first)
		if i > 0 && f.Delay > 0 {
			select {
			case <-ctx.Done():
				return wire.Usage{}, ctx.Err()
			case <-time.After(f.Delay):
			}
		}

		// Issue 3: Non-blocking context check before emit for deterministic priority
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
