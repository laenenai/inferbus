package org

import (
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// eventCodec implements es.Codec[*controlplanev1.OrgEvent] over the OrgEvent
// `oneof kind` wrapper message.
//
// es-lite's own codegen (examples/counter) makes E a sealed Go interface
// with one concrete type per oneof variant, so its codec marshals the
// variant itself and tags it with the variant's own proto full name. Our
// generated OrgEvent is instead a single wrapper message (Task 1's
// `oneof kind` idiom), so Encode first unwraps to find which variant is
// set, then marshals ONLY that inner message — never the wrapper — under a
// TypeURL naming the inner type (e.g. "controlplane.v1.OrgCreated", not
// "controlplane.v1.OrgEvent"). Decode reverses this: it unmarshals the
// payload into the concrete type the TypeURL names and re-wraps it into an
// OrgEvent before returning it, so the rest of the aggregate (Evolve,
// callers) only ever sees OrgEvent.
type eventCodec struct{}

var _ es.Codec[*controlplanev1.OrgEvent] = eventCodec{}

// Codec returns the org aggregate's event codec.
func Codec() es.Codec[*controlplanev1.OrgEvent] { return eventCodec{} }

func (eventCodec) Encode(e *controlplanev1.OrgEvent) (es.EncodedEvent, error) {
	var m proto.Message
	switch k := e.GetKind().(type) {
	case *controlplanev1.OrgEvent_Created:
		m = k.Created
	case *controlplanev1.OrgEvent_Renamed:
		m = k.Renamed
	case *controlplanev1.OrgEvent_MemberUpserted:
		m = k.MemberUpserted
	case *controlplanev1.OrgEvent_MemberRemoved:
		m = k.MemberRemoved
	case *controlplanev1.OrgEvent_ProjectCreated:
		m = k.ProjectCreated
	case *controlplanev1.OrgEvent_ProjectArchived:
		m = k.ProjectArchived
	default:
		return es.EncodedEvent{}, fmt.Errorf("org: encode: unset or unknown OrgEvent kind %T", e.GetKind())
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return es.EncodedEvent{}, err
	}
	typeURL := string(m.ProtoReflect().Descriptor().FullName())
	return es.EncodedEvent{TypeURL: typeURL, SchemaVersion: 1, Payload: b}, nil
}

func (eventCodec) Decode(enc es.EncodedEvent) (*controlplanev1.OrgEvent, error) {
	switch enc.TypeURL {
	case "controlplane.v1.OrgCreated":
		var m controlplanev1.OrgCreated
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_Created{Created: &m}}, nil

	case "controlplane.v1.OrgRenamed":
		var m controlplanev1.OrgRenamed
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_Renamed{Renamed: &m}}, nil

	case "controlplane.v1.MemberUpserted":
		var m controlplanev1.MemberUpserted
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_MemberUpserted{MemberUpserted: &m}}, nil

	case "controlplane.v1.MemberRemoved":
		var m controlplanev1.MemberRemoved
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_MemberRemoved{MemberRemoved: &m}}, nil

	case "controlplane.v1.ProjectCreated":
		var m controlplanev1.ProjectCreated
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_ProjectCreated{ProjectCreated: &m}}, nil

	case "controlplane.v1.ProjectArchived":
		var m controlplanev1.ProjectArchived
		if err := proto.Unmarshal(enc.Payload, &m); err != nil {
			return nil, err
		}
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_ProjectArchived{ProjectArchived: &m}}, nil

	default:
		return nil, fmt.Errorf("%w: %q", es.ErrUnknownEventType, enc.TypeURL)
	}
}
