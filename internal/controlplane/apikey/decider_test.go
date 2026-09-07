package apikey_test

import (
	"context"
	"errors"
	"testing"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// newRuntime stands up a fresh in-memory sqlite store + apikey runtime, one
// per test (unique DB name derived from t.Name(), shared cache so the single
// connection keeps it alive for the test's lifetime). Mirrors the org
// aggregate's harness (Task 2), itself mirroring es-lite's
// examples/counter harness.
func newRuntime(t *testing.T) *aggregate.Runtime[*controlplanev1.ApiKey, *controlplanev1.ApiKeyCommand, *controlplanev1.ApiKeyEvent] {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
}

func sid(t *testing.T, id string) es.StreamID {
	t.Helper()
	s, err := es.NewStreamID(apikey.StreamType, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return s
}

func createCmd(id, org, project, name, hash string, allow []string, rpm int32, budget int64) *controlplanev1.ApiKeyCommand {
	return &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Create{
		Create: &controlplanev1.CreateKey{
			Id:                 id,
			Org:                org,
			Project:            project,
			Name:               name,
			Hash:               hash,
			Allow:              allow,
			RateLimitRpm:       rpm,
			MonthlyTokenBudget: budget,
		},
	}}
}

func rotateCmd(newHash string) *controlplanev1.ApiKeyCommand {
	return &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Rotate{
		Rotate: &controlplanev1.RotateKey{NewHash: newHash},
	}}
}

func disableCmd() *controlplanev1.ApiKeyCommand {
	return &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Disable{
		Disable: &controlplanev1.DisableKey{},
	}}
}

func setAllowlistCmd(allow []string) *controlplanev1.ApiKeyCommand {
	return &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_SetAllowlist{
		SetAllowlist: &controlplanev1.SetAllowlist{Allow: allow},
	}}
}

func setLimitsCmd(rpm int32, budget int64) *controlplanev1.ApiKeyCommand {
	return &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_SetLimits{
		SetLimits: &controlplanev1.SetLimits{RateLimitRpm: rpm, MonthlyTokenBudget: budget},
	}}
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	res, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", []string{"model-a"}, 60, 1_000_000), es.Meta{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	s := res.State
	if s.GetId() != "key-1" || s.GetOrg() != "acme" || s.GetProject() != "proj-1" || s.GetName() != "prod key" {
		t.Fatalf("state after create = %+v, want id/org/project/name set", s)
	}
	if s.GetHash() != "hash-1" {
		t.Fatalf("hash after create = %q, want hash-1", s.GetHash())
	}
	if len(s.GetAllow()) != 1 || s.GetAllow()[0] != "model-a" {
		t.Fatalf("allow after create = %v, want [model-a]", s.GetAllow())
	}
	if s.GetRateLimitRpm() != 60 || s.GetMonthlyTokenBudget() != 1_000_000 {
		t.Fatalf("limits after create = rpm=%d budget=%d, want 60/1000000", s.GetRateLimitRpm(), s.GetMonthlyTokenBudget())
	}
	if s.GetDisabled() {
		t.Fatalf("disabled after create = true, want false")
	}
	if res.ToVersion != 1 {
		t.Fatalf("version = %d, want 1", res.ToVersion)
	}

	// Create twice on the same stream fails.
	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1_000_000), es.Meta{}); !errors.Is(err, apikey.ErrAlreadyExists) {
		t.Fatalf("create twice: got %v, want ErrAlreadyExists", err)
	}
}

func TestCommandsBeforeCreateFail(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, rotateCmd("hash-2"), es.Meta{}); !errors.Is(err, apikey.ErrNotFound) {
		t.Fatalf("rotate before create: got %v, want ErrNotFound", err)
	}
	if _, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{}); !errors.Is(err, apikey.ErrNotFound) {
		t.Fatalf("disable before create: got %v, want ErrNotFound", err)
	}
	if _, err := rt.Handle(ctx, stream, setAllowlistCmd([]string{"model-a"}), es.Meta{}); !errors.Is(err, apikey.ErrNotFound) {
		t.Fatalf("set allowlist before create: got %v, want ErrNotFound", err)
	}
	if _, err := rt.Handle(ctx, stream, setLimitsCmd(10, 1000), es.Meta{}); !errors.Is(err, apikey.ErrNotFound) {
		t.Fatalf("set limits before create: got %v, want ErrNotFound", err)
	}
}

// TestRotate proves rotate emits KeyRotated carrying the retired
// (previous) hash copied FROM STATE, and updates state.hash to the new
// value.
func TestRotate(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := rt.Handle(ctx, stream, rotateCmd("hash-2"), es.Meta{})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("rotate events = %d, want 1", len(res.Events))
	}
	rotated, ok := res.Events[0].GetKind().(*controlplanev1.ApiKeyEvent_Rotated)
	if !ok {
		t.Fatalf("rotate event kind = %T, want ApiKeyEvent_Rotated", res.Events[0].GetKind())
	}
	if rotated.Rotated.GetNewHash() != "hash-2" {
		t.Fatalf("KeyRotated.new_hash = %q, want hash-2", rotated.Rotated.GetNewHash())
	}
	if rotated.Rotated.GetPreviousHash() != "hash-1" {
		t.Fatalf("KeyRotated.previous_hash = %q, want hash-1 (copied from state)", rotated.Rotated.GetPreviousHash())
	}
	if res.State.GetHash() != "hash-2" {
		t.Fatalf("state.hash after rotate = %q, want hash-2", res.State.GetHash())
	}
}

// TestDisable proves disable emits KeyDisabled carrying the current hash
// copied FROM STATE, and marks state disabled.
func TestDisable(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{})
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("disable events = %d, want 1", len(res.Events))
	}
	disabled, ok := res.Events[0].GetKind().(*controlplanev1.ApiKeyEvent_Disabled)
	if !ok {
		t.Fatalf("disable event kind = %T, want ApiKeyEvent_Disabled", res.Events[0].GetKind())
	}
	if disabled.Disabled.GetCurrentHash() != "hash-1" {
		t.Fatalf("KeyDisabled.current_hash = %q, want hash-1 (copied from state)", disabled.Disabled.GetCurrentHash())
	}
	if !res.State.GetDisabled() {
		t.Fatalf("state.disabled after disable = false, want true")
	}
}

// TestDisableOnAlreadyDisabledIsNoOp asserts the controller's binding
// ruling from Task 2's review: DisableKey on an already-disabled key is a
// true no-op — zero events, nil error, no version bump — NOT ErrDisabled.
// ErrDisabled is reserved for real mutations (rotate/allowlist/limits) on a
// disabled key.
func TestDisableOnAlreadyDisabledIsNoOp(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{}); err != nil {
		t.Fatalf("disable: %v", err)
	}

	before, versionBefore, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load before: %v", err)
	}

	for i := 0; i < 2; i++ {
		res, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{})
		if err != nil {
			t.Fatalf("disable-on-disabled (attempt %d): got err %v, want nil", i, err)
		}
		if len(res.Events) != 0 {
			t.Fatalf("disable-on-disabled (attempt %d): %d events, want 0", i, len(res.Events))
		}
		if res.FromVersion != res.ToVersion {
			t.Fatalf("disable-on-disabled (attempt %d): FromVersion=%d ToVersion=%d, want equal (no append)", i, res.FromVersion, res.ToVersion)
		}
	}

	after, versionAfter, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	if versionAfter != versionBefore {
		t.Fatalf("version after no-op disables = %d, want unchanged %d", versionAfter, versionBefore)
	}
	if !after.GetDisabled() {
		t.Fatalf("state.disabled after no-op disables = false, want true (unchanged)")
	}
	_ = before
}

// TestRotateAfterDisableFails proves rotate — a real mutation — is rejected
// with ErrDisabled once the key is disabled.
func TestRotateAfterDisableFails(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{}); err != nil {
		t.Fatalf("disable: %v", err)
	}

	if _, err := rt.Handle(ctx, stream, rotateCmd("hash-2"), es.Meta{}); !errors.Is(err, apikey.ErrDisabled) {
		t.Fatalf("rotate after disable: got %v, want ErrDisabled", err)
	}
}

// TestAllowlistAndLimitsAfterDisableFail proves the other two real
// mutations (allowlist, limits) are likewise rejected with ErrDisabled once
// the key is disabled — the binding ruling names all three (rotate/
// allowlist/limits) as real mutations, distinct from disable-on-disabled's
// true no-op.
func TestAllowlistAndLimitsAfterDisableFail(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", nil, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, disableCmd(), es.Meta{}); err != nil {
		t.Fatalf("disable: %v", err)
	}

	if _, err := rt.Handle(ctx, stream, setAllowlistCmd([]string{"model-b"}), es.Meta{}); !errors.Is(err, apikey.ErrDisabled) {
		t.Fatalf("set allowlist after disable: got %v, want ErrDisabled", err)
	}
	if _, err := rt.Handle(ctx, stream, setLimitsCmd(10, 500), es.Meta{}); !errors.Is(err, apikey.ErrDisabled) {
		t.Fatalf("set limits after disable: got %v, want ErrDisabled", err)
	}
}

// TestAllowlistAndLimitsFoldIntoState exercises SetAllowlist/SetLimits
// success paths and confirms a fresh Load (forcing the codec's Decode
// path) reconstructs the updated values.
func TestAllowlistAndLimitsFoldIntoState(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	if _, err := rt.Handle(ctx, stream, createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", []string{"model-a"}, 60, 1000), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := rt.Handle(ctx, stream, setAllowlistCmd([]string{"model-a", "model-b"}), es.Meta{})
	if err != nil {
		t.Fatalf("set allowlist: %v", err)
	}
	if len(res.State.GetAllow()) != 2 {
		t.Fatalf("allow after set allowlist = %v, want 2 entries", res.State.GetAllow())
	}

	res, err = rt.Handle(ctx, stream, setLimitsCmd(120, 2_000_000), es.Meta{})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}
	if res.State.GetRateLimitRpm() != 120 || res.State.GetMonthlyTokenBudget() != 2_000_000 {
		t.Fatalf("limits after set limits = rpm=%d budget=%d, want 120/2000000", res.State.GetRateLimitRpm(), res.State.GetMonthlyTokenBudget())
	}

	// Reload from the log to prove the updates survive encode/decode.
	state, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(state.GetAllow()) != 2 || state.GetAllow()[0] != "model-a" || state.GetAllow()[1] != "model-b" {
		t.Fatalf("reloaded allow = %v, want [model-a model-b]", state.GetAllow())
	}
	if state.GetRateLimitRpm() != 120 || state.GetMonthlyTokenBudget() != 2_000_000 {
		t.Fatalf("reloaded limits = rpm=%d budget=%d, want 120/2000000", state.GetRateLimitRpm(), state.GetMonthlyTokenBudget())
	}
	if version != 3 {
		t.Fatalf("reloaded version = %d, want 3", version)
	}
}

// TestFullFold runs a full command sequence including a rotate before
// disable, and confirms Load — a fresh fold from the log — reconstructs
// the expected final state.
func TestFullFold(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "key-1")

	cmds := []*controlplanev1.ApiKeyCommand{
		createCmd("key-1", "acme", "proj-1", "prod key", "hash-1", []string{"model-a"}, 60, 1000),
		rotateCmd("hash-2"),
		setAllowlistCmd([]string{"model-a", "model-b"}),
		setLimitsCmd(30, 500),
		disableCmd(),
	}
	for _, c := range cmds {
		if _, err := rt.Handle(ctx, stream, c, es.Meta{}); err != nil {
			t.Fatalf("handle %T: %v", c.GetKind(), err)
		}
	}

	state, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if version != uint64(len(cmds)) {
		t.Fatalf("version = %d, want %d", version, len(cmds))
	}
	if state.GetHash() != "hash-2" {
		t.Fatalf("folded hash = %q, want hash-2", state.GetHash())
	}
	if len(state.GetAllow()) != 2 {
		t.Fatalf("folded allow = %v, want 2 entries", state.GetAllow())
	}
	if state.GetRateLimitRpm() != 30 || state.GetMonthlyTokenBudget() != 500 {
		t.Fatalf("folded limits = rpm=%d budget=%d, want 30/500", state.GetRateLimitRpm(), state.GetMonthlyTokenBudget())
	}
	if !state.GetDisabled() {
		t.Fatalf("folded disabled = false, want true")
	}
}
