// Package alias implements the "alias" aggregate for the inferbus control
// plane: one model/route alias's target and per-alias params. It is an
// es-lite Decider hand-written against the controlplane.v1 proto types
// generated in Task 1 (see api/controlplane/v1/alias.proto): AliasCommand
// and AliasEvent are `oneof kind` wrapper messages rather than es-lite's
// own sealed-interface codegen idiom (examples/counter), so Decide/Evolve
// switch on GetKind() and the codec (codec.go) maps each wrapped variant
// to its own TypeURL.
//
// SetAlias is an upsert: it applies uniformly whether the stream is fresh,
// already has a live alias, or was previously deleted (re-setting after a
// delete revives it) — there is no ErrAlreadyExists here, unlike org/apikey's
// Create. DeleteAlias, by contrast, is a true no-op (zero events, nil
// error) on a stream that never had an alias set OR whose alias is already
// deleted; Evolve keeps the last-known target/params around under
// deleted=true rather than clearing them, so a later revive has something
// to report if ever needed for audit.
//
// AliasCommand/AliasEvent do not carry scope or name — those come from the
// stream identity, not the payload — so this aggregate's state never sets
// Alias.Scope/Alias.Name itself; that is a concern for whatever composes
// the stream id (see StreamID below) and any read-model built from the
// stream identity, not for this package.
package alias

import (
	"errors"
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/wire"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// StreamType is the es.StreamID.Type for every alias stream.
const StreamType = "alias"

// ErrInvalidTarget is returned by SetAlias when the target has no
// NATS-subject-safe form at all: wire.Slug(target) == "".
//
// The target is a CONCRETE model name, and concrete model names are
// whatever the serving engine calls them — `llama3.2:latest` (Ollama),
// `meta-llama/Llama-3.2-1B-Instruct` (vLLM). Those are stored verbatim:
// the subject layer (wire.ReqSubject/wire.Durable) applies wire.Slug at
// the point of use on both sides of the wire, so requiring
// wire.Slug(target) == target here would make every real engine's models
// unaliasable — and would contradict worker.Discover, which registers
// engine ids exactly as reported. Only a target that slugs away to nothing
// is rejected, because it would name the dangling subject
// "inference.req." / durable "model-".
var ErrInvalidTarget = errors.New("alias: invalid target")

// StreamID composes a scope and alias name into the identifier other parts
// of the control plane (e.g. the Task 7 KV projector's bucket key) use to
// name one alias. It is a plain string, not an es.StreamID: es-lite's
// stream-id slug forbids "/", so whatever constructs the actual
// es.StreamID for this aggregate's runtime encodes scope/name some other
// way — that composition is out of scope for this package (see task-12's
// binding note: "StreamType constants unused outside their packages except
// stream-id construction in runtimes"). The es-lite stream-id encoding
// itself is owned by the admin API layer (Task 10); the binding encoding
// (superseding an earlier "scope + '_' + name" draft, which collided with
// the slug regex's ^[a-z0-9] requirement for the global scope) is:
//
//   - global scope: "g_<name>"
//   - org scope:    "o_<orgid>_<name>"
//
// unambiguous because org ids and names are always wire.Slug-safe and
// never contain "_" (controller ruling). Task 7's
// controlplane.SplitAliasStreamID parses this encoding back into
// (scope, name) for the KV projector's bucket key.
func StreamID(scope, name string) string {
	return scope + "/" + name
}

// Decider is the alias aggregate's business logic. State is the Alias
// proto message used directly; an empty, non-deleted Target means the
// stream has no alias set yet.
var Decider = es.Decider[*controlplanev1.Alias, *controlplanev1.AliasCommand, *controlplanev1.AliasEvent]{
	Initial: func() *controlplanev1.Alias { return &controlplanev1.Alias{} },

	Decide: func(s *controlplanev1.Alias, cmd *controlplanev1.AliasCommand) ([]*controlplanev1.AliasEvent, []es.ConstraintOp, error) {
		switch k := cmd.GetKind().(type) {

		case *controlplanev1.AliasCommand_Set:
			target := k.Set.GetTarget()
			if wire.Slug(target) == "" {
				// Covers the empty target too, which must be rejected: a
				// live alias with an empty target would be
				// indistinguishable from "never set" under the delete
				// no-op sentinel below (s.GetTarget() == ""), making it
				// silently undeletable forever. Because every accepted
				// target has a non-empty slug, it is itself non-empty, so
				// that sentinel stays sound.
				return nil, nil, ErrInvalidTarget
			}
			// Upsert: unconditionally emits AliasSet, whether the stream
			// is fresh, already has a live alias, or was previously
			// deleted (revival).
			return []*controlplanev1.AliasEvent{
				wrap(&controlplanev1.AliasSet{
					Target: target,
					Params: k.Set.GetParams(),
				}),
			}, nil, nil

		case *controlplanev1.AliasCommand_Delete:
			if s.GetTarget() == "" || s.GetDeleted() {
				// Never set, or already deleted: nothing changes. A no-op
				// command emits zero events and no error — the log is an
				// audit trail, never a place for spurious no-op events.
				// (Binding controller ruling: both cases are a true
				// no-op.)
				return nil, nil, nil
			}
			return []*controlplanev1.AliasEvent{
				wrap(&controlplanev1.AliasDeleted{}),
			}, nil, nil

		default:
			return nil, nil, fmt.Errorf("alias: unknown command kind %T", cmd.GetKind())
		}
	},

	Evolve: func(s *controlplanev1.Alias, e *controlplanev1.AliasEvent) *controlplanev1.Alias {
		ns, ok := proto.Clone(s).(*controlplanev1.Alias)
		if !ok {
			// proto.Clone on an Alias always yields an Alias; unreachable.
			ns = &controlplanev1.Alias{}
		}

		switch k := e.GetKind().(type) {

		case *controlplanev1.AliasEvent_Set:
			ns.Target = k.Set.GetTarget()
			// Defensive copy: the event's Params map aliases the
			// command's (and thus, ultimately, the caller's) backing
			// map. Without a copy, mutating the caller's map after
			// Handle returns would mutate Result.State. (Binding
			// ruling: maps alias like slices.)
			ns.Params = copyParams(k.Set.GetParams())
			ns.Deleted = false

		case *controlplanev1.AliasEvent_Deleted:
			ns.Deleted = true
		}

		return ns
	},
}

// wrap lifts a concrete event payload into the AliasEvent oneof wrapper.
func wrap(payload any) *controlplanev1.AliasEvent {
	switch p := payload.(type) {
	case *controlplanev1.AliasSet:
		return &controlplanev1.AliasEvent{Kind: &controlplanev1.AliasEvent_Set{Set: p}}
	case *controlplanev1.AliasDeleted:
		return &controlplanev1.AliasEvent{Kind: &controlplanev1.AliasEvent_Deleted{Deleted: p}}
	default:
		panic(fmt.Sprintf("alias: wrap: unknown event payload %T", payload))
	}
}

// copyParams returns a defensive copy of m, preserving nil.
func copyParams(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
