package apikey

import (
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// eventCodec implements es.Codec[*controlplanev1.ApiKeyEvent] over the
// ApiKeyEvent `oneof kind` wrapper message.
//
// es-lite's own codegen (examples/counter) makes E a sealed Go interface
// with one concrete type per oneof variant, so its codec marshals the
// variant itself and tags it with the variant's own proto full name. Our
// generated ApiKeyEvent is instead a single wrapper message (Task 1's
// `oneof kind` idiom), so Encode first unwraps to find which variant is
// set, then marshals ONLY that inner message — never the wrapper — under a
// TypeURL naming the inner type (e.g. "controlplane.v1.KeyCreated", not
// "controlplane.v1.ApiKeyEvent"). Decode reverses this: it unmarshals the
// payload into the concrete type the TypeURL names and re-wraps it into an
// ApiKeyEvent before returning it, so the rest of the aggregate (Evolve,
// callers) only ever sees ApiKeyEvent.
type eventCodec struct{}

var _ es.Codec[*controlplanev1.ApiKeyEvent] = eventCodec{}

// Codec returns the apikey aggregate's event codec.
func Codec() es.Codec[*controlplanev1.ApiKeyEvent] { return eventCodec{} }

func (eventCodec) Encode(e *controlplanev1.ApiKeyEvent) (es.EncodedEvent, error) {
	var m proto.Message
	switch k := e.GetKind().(type) {
	case *controlplanev1.ApiKeyEvent_Created:
		m = k.Created
	case *controlplanev1.ApiKeyEvent_Rotated:
		m = k.Rotated
	case *controlplanev1.ApiKeyEvent_Disabled:
		m = k.Disabled
	case *controlplanev1.ApiKeyEvent_AllowlistChanged:
		m = k.AllowlistChanged
	case *controlplanev1.ApiKeyEvent_LimitsChanged:
		m = k.LimitsChanged
	default:
		return es.EncodedEvent{}, fmt.Errorf("apikey: encode: unset or unknown ApiKeyEvent kind %T", e.GetKind())
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return es.EncodedEvent{}, err
	}
	typeURL := string(m.ProtoReflect().Descriptor().FullName())
	return es.EncodedEvent{TypeURL: typeURL, SchemaVersion: 1, Payload: b}, nil
}

func (eventCodec) Decode(enc es.EncodedEvent) (*controlplanev1.ApiKeyEvent, error) {
	switch enc.TypeURL {
	case "controlplane.v1.KeyCreated":
		var m controlplanev1.KeyCreated
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Created{Created: &m}}, nil

	case "controlplane.v1.KeyRotated":
		var m controlplanev1.KeyRotated
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Rotated{Rotated: &m}}, nil

	case "controlplane.v1.KeyDisabled":
		var m controlplanev1.KeyDisabled
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Disabled{Disabled: &m}}, nil

	case "controlplane.v1.KeyAllowlistChanged":
		var m controlplanev1.KeyAllowlistChanged
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_AllowlistChanged{AllowlistChanged: &m}}, nil

	case "controlplane.v1.KeyLimitsChanged":
		var m controlplanev1.KeyLimitsChanged
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_LimitsChanged{LimitsChanged: &m}}, nil

	default:
		return nil, fmt.Errorf("%w: %q", es.ErrUnknownEventType, enc.TypeURL)
	}
}
