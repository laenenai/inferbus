package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/infbus/infbus/internal/wire"
)

func TestFakeEngineErrEmptyChunks(t *testing.T) {
	// (a) Err + empty Chunks returns Err with zero emits
	testErr := errors.New("test error")
	fe := &FakeEngine{
		Chunks:     []string{},
		FinalUsage: wire.Usage{PromptTokens: 10, CompletionTokens: 20},
		Err:        testErr,
	}

	var emitCount int
	usage, err := fe.ChatStream(context.Background(), "test-model", nil, func(msg json.RawMessage) error {
		emitCount++
		return nil
	})

	if err != testErr {
		t.Errorf("expected error %v, got %v", testErr, err)
	}
	if emitCount != 0 {
		t.Errorf("expected 0 emits, got %d", emitCount)
	}
	if usage != (wire.Usage{}) {
		t.Errorf("expected zero usage on error, got %v", usage)
	}
}

func TestFakeEngineCanceledCtxDelay0(t *testing.T) {
	// (b) Already-canceled ctx with Delay 0 returns ctx.Err() with zero emits
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fe := &FakeEngine{
		Chunks:     []string{"chunk1", "chunk2", "chunk3"},
		FinalUsage: wire.Usage{PromptTokens: 10, CompletionTokens: 5},
		Err:        nil,
		Delay:      0, // Deterministic: no blocking delay
	}

	var emitCount int
	usage, err := fe.ChatStream(ctx, "test-model", nil, func(msg json.RawMessage) error {
		emitCount++
		return nil
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if emitCount != 0 {
		t.Errorf("expected 0 emits with canceled context, got %d", emitCount)
	}
	if usage != (wire.Usage{}) {
		t.Errorf("expected zero usage on cancellation, got %v", usage)
	}
}

func TestFakeEngineHappyPath(t *testing.T) {
	// (c) Happy path with 3 chunks emits exactly 3 then FinalUsage
	fe := &FakeEngine{
		Chunks:     []string{"chunk1", "chunk2", "chunk3"},
		FinalUsage: wire.Usage{PromptTokens: 15, CompletionTokens: 8},
		Err:        nil,
		Delay:      0,
	}

	var emitted []string
	usage, err := fe.ChatStream(context.Background(), "test-model", nil, func(msg json.RawMessage) error {
		emitted = append(emitted, string(msg))
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(emitted) != 3 {
		t.Errorf("expected 3 emits, got %d: %v", len(emitted), emitted)
	}
	if emitted[0] != "chunk1" || emitted[1] != "chunk2" || emitted[2] != "chunk3" {
		t.Errorf("unexpected chunks emitted: %v", emitted)
	}
	if usage != (wire.Usage{PromptTokens: 15, CompletionTokens: 8}) {
		t.Errorf("expected FinalUsage %v, got %v", fe.FinalUsage, usage)
	}
}
