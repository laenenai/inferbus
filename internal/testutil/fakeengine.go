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
	for i, c := range f.Chunks {
		if f.Err != nil && i == len(f.Chunks)/2 {
			return wire.Usage{}, f.Err
		}
		select {
		case <-ctx.Done():
			return wire.Usage{}, ctx.Err()
		case <-time.After(f.Delay):
		}
		if err := emit(json.RawMessage(c)); err != nil {
			return wire.Usage{}, err
		}
	}
	return f.FinalUsage, nil
}
