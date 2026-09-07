package alias

import (
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// eventCodec implements es.Codec[*controlplanev1.AliasEvent] over the
// AliasEvent `oneof kind` wrapper message.
//
// es-lite's own codegen (examples/counter) makes E a sealed Go interface
// with one concrete type per oneof variant, so its codec marshals the
// variant itself and tags it with the variant's own proto full name. Our
// generated AliasEvent is instead a single wrapper message (Task 1's
// `oneof kind` idiom), so Encode first unwraps to find which variant is
// set, then marshals ONLY that inner message — never the wrapper — under a
// TypeURL naming the inner type (e.g. "controlplane.v1.AliasSet", not
// "controlplane.v1.AliasEvent"). Decode reverses this: it unmarshals the
// payload into the concrete type the TypeURL names and re-wraps it into an
// AliasEvent before returning it, so the rest of the aggregate (Evolve,
// callers) only ever sees AliasEvent.
type eventCodec struct{}

var _ es.Codec[*controlplanev1.AliasEvent] = eventCodec{}

// Codec returns the alias aggregate's event codec.
func Codec() es.Codec[*controlplanev1.AliasEvent] { return eventCodec{} }

func (eventCodec) Encode(e *controlplanev1.AliasEvent) (es.EncodedEvent, error) {
	var m proto.Message
	switch k := e.GetKind().(type) {
	case *controlplanev1.AliasEvent_Set:
		m = k.Set
	case *controlplanev1.AliasEvent_Deleted:
		m = k.Deleted
	default:
		return es.EncodedEvent{}, fmt.Errorf("alias: encode: unset or unknown AliasEvent kind %T", e.GetKind())
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return es.EncodedEvent{}, err
	}
	typeURL := string(m.ProtoReflect().Descriptor().FullName())
	return es.EncodedEvent{TypeURL: typeURL, SchemaVersion: 1, Payload: b}, nil
}

func (eventCodec) Decode(enc es.EncodedEvent) (*controlplanev1.AliasEvent, error) {
	switch enc.TypeURL {
	case "controlplane.v1.AliasSet":
		var m controlplanev1.AliasSet
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.AliasEvent{Kind: &controlplanev1.AliasEvent_Set{Set: &m}}, nil

	case "controlplane.v1.AliasDeleted":
		var m controlplanev1.AliasDeleted
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.AliasEvent{Kind: &controlplanev1.AliasEvent_Deleted{Deleted: &m}}, nil

	default:
		return nil, fmt.Errorf("%w: %q", es.ErrUnknownEventType, enc.TypeURL)
	}
}
