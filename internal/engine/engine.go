// Package engine defines the worker-side seam to model backends.
package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/infbus/infbus/internal/wire"
)

type Engine interface {
	Chat(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error)

	// ChatStream streams a response, calling emit once per chunk in
	// generation order. emit MUST be called synchronously, from a single
	// goroutine (the same goroutine that called ChatStream, or one it
	// itself sequences calls through) — never concurrently, and never in a
	// way that lets one emit call return before the previous one has. The
	// worker relays each emit call straight into a wire frame with the
	// next sequence number in call order (see internal/worker.handle's
	// `publish` closure and its non-atomic `seq++`); that sequencing is
	// unsynchronized by design, on the assumption that an Engine
	// implementation upholds this contract. A concurrent or out-of-order
	// emit will corrupt the frame sequence the gateway relies on to detect
	// dropped frames (internal/relay.Listener.Next's seq-gap check).
	ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error)
}

// Error is an upstream engine failure with an HTTP-mappable status.
type Error struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *Error) Error() string { return fmt.Sprintf("engine %s (%d): %s", e.Code, e.HTTPStatus, e.Message) }
