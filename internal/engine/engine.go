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
	ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error)
}

// Error is an upstream engine failure with an HTTP-mappable status.
type Error struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *Error) Error() string { return fmt.Sprintf("engine %s (%d): %s", e.Code, e.HTTPStatus, e.Message) }
