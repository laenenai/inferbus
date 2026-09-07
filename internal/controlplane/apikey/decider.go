// Package apikey implements the "apikey" aggregate for the inferbus
// control plane: one API key's identity, allowlist, rate/budget limits, and
// disabled flag. Like the org aggregate (Task 2), it is an es-lite Decider
// hand-written against the controlplane.v1 proto types generated in Task 1
// (see api/controlplane/v1/apikey.proto): ApiKeyCommand and ApiKeyEvent are
// `oneof kind` wrapper messages rather than es-lite's own sealed-interface
// codegen idiom (examples/counter), so Decide/Evolve switch on GetKind()
// and the codec (codec.go) maps each wrapped variant to its own TypeURL.
//
// State carries only the key's hash, never plaintext — see
// internal/controlplane/keys.go for hashing. KeyRotated/KeyDisabled events
// carry the RETIRED hash copied from state at decide time; commands never
// carry a retired hash themselves (CreateKey/RotateKey only ever supply the
// new hash the caller minted).
package apikey

import (
	"errors"
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// StreamType is the es.StreamID.Type for every apikey stream.
const StreamType = "apikey"

// Domain errors, surfaced verbatim by aggregate.Runtime.Handle when Decide
// rejects a command.
var (
	// ErrAlreadyExists is returned by CreateKey when the stream already has
	// a key (Id is non-empty).
	ErrAlreadyExists = errors.New("apikey: already exists")

	// ErrNotFound is returned by every command other than CreateKey when
	// issued against a stream with no key yet (Id is empty).
	ErrNotFound = errors.New("apikey: not found")

	// ErrDisabled is returned by a real mutation (RotateKey, SetAllowlist,
	// SetLimits) issued against a key that is already disabled. Disabling
	// an already-disabled key is a separate case — a true no-op (zero
	// events, nil error), never ErrDisabled — since it changes nothing.
	ErrDisabled = errors.New("apikey: disabled")
)

// Decider is the apikey aggregate's business logic. State is the ApiKey
// proto message used directly; an empty Id means the stream has no key
// yet.
var Decider = es.Decider[*controlplanev1.ApiKey, *controlplanev1.ApiKeyCommand, *controlplanev1.ApiKeyEvent]{
	Initial: func() *controlplanev1.ApiKey { return &controlplanev1.ApiKey{} },

	Decide: func(s *controlplanev1.ApiKey, cmd *controlplanev1.ApiKeyCommand) ([]*controlplanev1.ApiKeyEvent, []es.ConstraintOp, error) {
		switch k := cmd.GetKind().(type) {

		case *controlplanev1.ApiKeyCommand_Create:
			if s.GetId() != "" {
				return nil, nil, ErrAlreadyExists
			}
			c := k.Create
			return []*controlplanev1.ApiKeyEvent{
				wrap(&controlplanev1.KeyCreated{
					Id:                 c.GetId(),
					Org:                c.GetOrg(),
					Project:            c.GetProject(),
					Name:               c.GetName(),
					Hash:               c.GetHash(),
					Allow:              c.GetAllow(),
					RateLimitRpm:       c.GetRateLimitRpm(),
					MonthlyTokenBudget: c.GetMonthlyTokenBudget(),
				}),
			}, nil, nil

		case *controlplanev1.ApiKeyCommand_Rotate:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			if s.GetDisabled() {
				return nil, nil, ErrDisabled
			}
			return []*controlplanev1.ApiKeyEvent{
				wrap(&controlplanev1.KeyRotated{
					NewHash:      k.Rotate.GetNewHash(),
					PreviousHash: s.GetHash(),
				}),
			}, nil, nil

		case *controlplanev1.ApiKeyCommand_Disable:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			if s.GetDisabled() {
				// Already disabled: nothing changes. A no-op command emits
				// zero events and no error — the log is an audit trail,
				// never a place for spurious no-op events. (Binding
				// controller ruling, Task 2 review: this is distinct from
				// ErrDisabled, which rejects real mutations.)
				return nil, nil, nil
			}
			return []*controlplanev1.ApiKeyEvent{
				wrap(&controlplanev1.KeyDisabled{CurrentHash: s.GetHash()}),
			}, nil, nil

		case *controlplanev1.ApiKeyCommand_SetAllowlist:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			if s.GetDisabled() {
				return nil, nil, ErrDisabled
			}
			return []*controlplanev1.ApiKeyEvent{
				wrap(&controlplanev1.KeyAllowlistChanged{Allow: k.SetAllowlist.GetAllow()}),
			}, nil, nil

		case *controlplanev1.ApiKeyCommand_SetLimits:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			if s.GetDisabled() {
				return nil, nil, ErrDisabled
			}
			return []*controlplanev1.ApiKeyEvent{
				wrap(&controlplanev1.KeyLimitsChanged{
					RateLimitRpm:       k.SetLimits.GetRateLimitRpm(),
					MonthlyTokenBudget: k.SetLimits.GetMonthlyTokenBudget(),
				}),
			}, nil, nil

		default:
			return nil, nil, fmt.Errorf("apikey: unknown command kind %T", cmd.GetKind())
		}
	},

	Evolve: func(s *controlplanev1.ApiKey, e *controlplanev1.ApiKeyEvent) *controlplanev1.ApiKey {
		ns, ok := proto.Clone(s).(*controlplanev1.ApiKey)
		if !ok {
			// proto.Clone on an ApiKey always yields an ApiKey; unreachable.
			ns = &controlplanev1.ApiKey{}
		}

		switch k := e.GetKind().(type) {

		case *controlplanev1.ApiKeyEvent_Created:
			c := k.Created
			ns.Id = c.GetId()
			ns.Org = c.GetOrg()
			ns.Project = c.GetProject()
			ns.Name = c.GetName()
			ns.Hash = c.GetHash()
			ns.Allow = c.GetAllow()
			ns.RateLimitRpm = c.GetRateLimitRpm()
			ns.MonthlyTokenBudget = c.GetMonthlyTokenBudget()
			ns.Disabled = false

		case *controlplanev1.ApiKeyEvent_Rotated:
			ns.Hash = k.Rotated.GetNewHash()

		case *controlplanev1.ApiKeyEvent_Disabled:
			ns.Disabled = true

		case *controlplanev1.ApiKeyEvent_AllowlistChanged:
			ns.Allow = k.AllowlistChanged.GetAllow()

		case *controlplanev1.ApiKeyEvent_LimitsChanged:
			ns.RateLimitRpm = k.LimitsChanged.GetRateLimitRpm()
			ns.MonthlyTokenBudget = k.LimitsChanged.GetMonthlyTokenBudget()
		}

		return ns
	},
}

// wrap lifts a concrete event payload into the ApiKeyEvent oneof wrapper.
func wrap(payload any) *controlplanev1.ApiKeyEvent {
	switch p := payload.(type) {
	case *controlplanev1.KeyCreated:
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Created{Created: p}}
	case *controlplanev1.KeyRotated:
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Rotated{Rotated: p}}
	case *controlplanev1.KeyDisabled:
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_Disabled{Disabled: p}}
	case *controlplanev1.KeyAllowlistChanged:
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_AllowlistChanged{AllowlistChanged: p}}
	case *controlplanev1.KeyLimitsChanged:
		return &controlplanev1.ApiKeyEvent{Kind: &controlplanev1.ApiKeyEvent_LimitsChanged{LimitsChanged: p}}
	default:
		panic(fmt.Sprintf("apikey: wrap: unknown event payload %T", payload))
	}
}
