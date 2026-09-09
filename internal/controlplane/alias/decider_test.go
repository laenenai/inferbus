package alias_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/alias"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// newRuntime stands up a fresh in-memory sqlite store + alias runtime, one
// per test (unique DB name derived from t.Name(), shared cache so the
// single connection keeps it alive for the test's lifetime). Mirrors the
// org/apikey aggregates' harness (Tasks 2-3), itself mirroring es-lite's
// examples/counter harness.
func newRuntime(t *testing.T) *aggregate.Runtime[*controlplanev1.Alias, *controlplanev1.AliasCommand, *controlplanev1.AliasEvent] {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return aggregate.NewRuntime(store, alias.Decider, alias.Codec())
}

func sid(t *testing.T, id string) es.StreamID {
	t.Helper()
	s, err := es.NewStreamID(alias.StreamType, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return s
}

func setCmd(target string, params map[string]string) *controlplanev1.AliasCommand {
	return &controlplanev1.AliasCommand{Kind: &controlplanev1.AliasCommand_Set{
		Set: &controlplanev1.SetAlias{Target: target, Params: params},
	}}
}

func deleteCmd() *controlplanev1.AliasCommand {
	return &controlplanev1.AliasCommand{Kind: &controlplanev1.AliasCommand_Delete{
		Delete: &controlplanev1.DeleteAlias{},
	}}
}

// TestStreamID proves the pure StreamID(scope, name) helper's exact format
// requirement from the brief.
func TestStreamID(t *testing.T) {
	if got, want := alias.StreamID("acme", "gpt-4"), "acme/gpt-4"; got != want {
		t.Fatalf("StreamID(acme, gpt-4) = %q, want %q", got, want)
	}
}

// TestSetAlias proves SetAlias on a fresh (never-set) stream emits an
// AliasSet event and folds into non-deleted state carrying the target.
func TestSetAlias(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	res, err := rt.Handle(ctx, stream, setCmd("model-a", map[string]string{"temp": "0.7"}), es.Meta{})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("set events = %d, want 1", len(res.Events))
	}
	set, ok := res.Events[0].GetKind().(*controlplanev1.AliasEvent_Set)
	if !ok {
		t.Fatalf("set event kind = %T, want AliasEvent_Set", res.Events[0].GetKind())
	}
	if set.Set.GetTarget() != "model-a" {
		t.Fatalf("AliasSet.target = %q, want model-a", set.Set.GetTarget())
	}
	if res.State.GetTarget() != "model-a" {
		t.Fatalf("state.target after set = %q, want model-a", res.State.GetTarget())
	}
	if res.State.GetDeleted() {
		t.Fatalf("state.deleted after set = true, want false")
	}
	if got := res.State.GetParams()["temp"]; got != "0.7" {
		t.Fatalf("state.params[temp] after set = %q, want 0.7", got)
	}
	if res.ToVersion != 1 {
		t.Fatalf("version = %d, want 1", res.ToVersion)
	}
}

// TestSetAliasUpsert proves SetAlias is a true upsert: setting again on an
// existing (not deleted) alias with a new target emits a second AliasSet
// and updates state, never an error.
func TestSetAliasUpsert(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	if _, err := rt.Handle(ctx, stream, setCmd("model-a", nil), es.Meta{}); err != nil {
		t.Fatalf("first set: %v", err)
	}

	res, err := rt.Handle(ctx, stream, setCmd("model-b", nil), es.Meta{})
	if err != nil {
		t.Fatalf("second set: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("second set events = %d, want 1", len(res.Events))
	}
	if _, ok := res.Events[0].GetKind().(*controlplanev1.AliasEvent_Set); !ok {
		t.Fatalf("second set event kind = %T, want AliasEvent_Set", res.Events[0].GetKind())
	}
	if res.State.GetTarget() != "model-b" {
		t.Fatalf("state.target after second set = %q, want model-b", res.State.GetTarget())
	}
	if res.ToVersion != 2 {
		t.Fatalf("version = %d, want 2", res.ToVersion)
	}
}

// TestSetAliasRevivesAfterDelete proves the brief's upsert semantics
// explicitly: re-setting a deleted alias revives it (a second AliasSet,
// state.deleted flips back to false).
func TestSetAliasRevivesAfterDelete(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	if _, err := rt.Handle(ctx, stream, setCmd("model-a", nil), es.Meta{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, deleteCmd(), es.Meta{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := rt.Handle(ctx, stream, setCmd("model-c", nil), es.Meta{})
	if err != nil {
		t.Fatalf("revive set: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("revive set events = %d, want 1", len(res.Events))
	}
	if _, ok := res.Events[0].GetKind().(*controlplanev1.AliasEvent_Set); !ok {
		t.Fatalf("revive event kind = %T, want AliasEvent_Set", res.Events[0].GetKind())
	}
	if res.State.GetDeleted() {
		t.Fatalf("state.deleted after revive = true, want false")
	}
	if res.State.GetTarget() != "model-c" {
		t.Fatalf("state.target after revive = %q, want model-c", res.State.GetTarget())
	}
}

// TestDeleteAlias proves DeleteAlias on a live alias emits AliasDeleted and
// marks state deleted.
func TestDeleteAlias(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	if _, err := rt.Handle(ctx, stream, setCmd("model-a", nil), es.Meta{}); err != nil {
		t.Fatalf("set: %v", err)
	}

	res, err := rt.Handle(ctx, stream, deleteCmd(), es.Meta{})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("delete events = %d, want 1", len(res.Events))
	}
	if _, ok := res.Events[0].GetKind().(*controlplanev1.AliasEvent_Deleted); !ok {
		t.Fatalf("delete event kind = %T, want AliasEvent_Deleted", res.Events[0].GetKind())
	}
	if !res.State.GetDeleted() {
		t.Fatalf("state.deleted after delete = false, want true")
	}
}

// TestDeleteOnNeverSetIsNoOp asserts the binding ruling: DeleteAlias on a
// never-set alias is a true no-op — zero events, nil error, no version
// bump — not an error.
func TestDeleteOnNeverSetIsNoOp(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	for i := 0; i < 2; i++ {
		res, err := rt.Handle(ctx, stream, deleteCmd(), es.Meta{})
		if err != nil {
			t.Fatalf("delete never-set (attempt %d): got err %v, want nil", i, err)
		}
		if len(res.Events) != 0 {
			t.Fatalf("delete never-set (attempt %d): %d events, want 0", i, len(res.Events))
		}
		if res.FromVersion != res.ToVersion {
			t.Fatalf("delete never-set (attempt %d): FromVersion=%d ToVersion=%d, want equal (no append)", i, res.FromVersion, res.ToVersion)
		}
	}

	_, version, err := rt.Load(ctx, stream)
	if err != nil {
		// Load returning an error for a stream with zero events is fine
		// either way for this assertion; the important invariant already
		// checked above is FromVersion == ToVersion == 0.
		return
	}
	if version != 0 {
		t.Fatalf("version after no-op deletes on never-set stream = %d, want 0", version)
	}
}

// TestDeleteOnAlreadyDeletedIsNoOp asserts the other half of the binding
// ruling: DeleteAlias on an already-deleted alias is likewise a true
// no-op, distinct from the live-delete path in TestDeleteAlias.
func TestDeleteOnAlreadyDeletedIsNoOp(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	if _, err := rt.Handle(ctx, stream, setCmd("model-a", nil), es.Meta{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, deleteCmd(), es.Meta{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	before, versionBefore, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load before: %v", err)
	}

	for i := 0; i < 2; i++ {
		res, err := rt.Handle(ctx, stream, deleteCmd(), es.Meta{})
		if err != nil {
			t.Fatalf("delete-on-deleted (attempt %d): got err %v, want nil", i, err)
		}
		if len(res.Events) != 0 {
			t.Fatalf("delete-on-deleted (attempt %d): %d events, want 0", i, len(res.Events))
		}
		if res.FromVersion != res.ToVersion {
			t.Fatalf("delete-on-deleted (attempt %d): FromVersion=%d ToVersion=%d, want equal (no append)", i, res.FromVersion, res.ToVersion)
		}
	}

	after, versionAfter, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	if versionAfter != versionBefore {
		t.Fatalf("version after no-op deletes = %d, want unchanged %d", versionAfter, versionBefore)
	}
	if !after.GetDeleted() {
		t.Fatalf("state.deleted after no-op deletes = false, want true (unchanged)")
	}
	_ = before
}

// TestSetAliasEmptyTargetRejected proves Decide rejects an empty target
// explicitly with ErrInvalidTarget rather than letting it slip through
// wire.Slug("") == "" — an empty-target alias would otherwise be
// indistinguishable from "never set" under the delete no-op sentinel,
// making it silently undeletable forever.
func TestSetAliasEmptyTargetRejected(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	res, err := rt.Handle(ctx, stream, setCmd("", nil), es.Meta{})
	if !errors.Is(err, alias.ErrInvalidTarget) {
		t.Fatalf("set empty target: got %v, want ErrInvalidTarget", err)
	}
	if len(res.Events) != 0 {
		t.Fatalf("set empty target: %d events, want 0", len(res.Events))
	}
}

// TestSetAliasRealEngineModelIDTarget: an alias must be able to target a
// model id exactly as a real engine reports it — Ollama's `name:tag`, a
// vLLM HuggingFace repo id — since that is what a zero-config worker
// registers (worker.Discover) and what the engine expects back. None of
// these is wire.Slug-identical to itself; the subject layer
// (wire.ReqSubject/wire.Durable) slugs them at the point of use, so the
// aggregate must store them verbatim rather than reject them.
func TestSetAliasRealEngineModelIDTarget(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	for _, target := range []string{
		"llama3.2:latest",
		"qwen2.5-coder:7b",
		"meta-llama/Llama-3.2-1B-Instruct",
	} {
		res, err := rt.Handle(ctx, stream, setCmd(target, nil), es.Meta{})
		if err != nil {
			t.Fatalf("set target %q: %v", target, err)
		}
		if len(res.Events) != 1 {
			t.Fatalf("set target %q: %d events, want 1", target, len(res.Events))
		}
		if got := res.State.GetTarget(); got != target {
			t.Fatalf("state target = %q, want %q verbatim (no slugging in the aggregate)", got, target)
		}
	}
}

// TestSetAliasInvalidTarget proves Decide rejects a SetAlias whose target
// has no subject-safe form at all (wire.Slug(target) == "") with
// ErrInvalidTarget, and that rejection produces zero events / no state
// change.
func TestSetAliasInvalidTarget(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	if _, err := rt.Handle(ctx, stream, setCmd("///", nil), es.Meta{}); !errors.Is(err, alias.ErrInvalidTarget) {
		t.Fatalf("set invalid target: got %v, want ErrInvalidTarget", err)
	}

	// State remains untouched (never set).
	_, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if version != 0 {
		t.Fatalf("version after rejected set = %d, want 0", version)
	}
}

// TestParamsRoundTrip proves the params map survives a set, a fresh Load
// (forcing the codec's Decode path), and contains exactly what was sent.
func TestParamsRoundTrip(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	params := map[string]string{"temperature": "0.2", "top_p": "0.9"}
	if _, err := rt.Handle(ctx, stream, setCmd("model-a", params), es.Meta{}); err != nil {
		t.Fatalf("set: %v", err)
	}

	state, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if version != 1 {
		t.Fatalf("version = %d, want 1", version)
	}
	if len(state.GetParams()) != 2 || state.GetParams()["temperature"] != "0.2" || state.GetParams()["top_p"] != "0.9" {
		t.Fatalf("reloaded params = %v, want map[temperature:0.2 top_p:0.9]", state.GetParams())
	}
}

// TestParamsMapNotAliased proves Evolve defensively copies the Params map
// rather than aliasing the caller's map: mutating the map passed into a
// command after Handle returns must never change Result.State. Binding
// ruling: maps alias like slices.
func TestParamsMapNotAliased(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-gpt-4")

	params := map[string]string{"temp": "0.7"}
	res, err := rt.Handle(ctx, stream, setCmd("model-a", params), es.Meta{})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	params["temp"] = "MUTATED"
	params["new-key"] = "sneaked-in"
	if got := res.State.GetParams()["temp"]; got != "0.7" {
		t.Fatalf("state.params[temp] after set = %q, want unaliased %q", got, "0.7")
	}
	if _, ok := res.State.GetParams()["new-key"]; ok {
		t.Fatalf("state.params after set contains sneaked-in key added post-Handle via aliasing")
	}
}

// TestSetAliasReservedParamRejected is Task 5: Decide must reject a
// SetAlias whose params contain a reserved key ("model" or "stream")
// with ErrReservedParam, producing zero events / no state change — the
// invariant lives in the aggregate so it holds for every command path,
// not just the admin HTTP handler. Matching is case-insensitive
// (binding: this fixed a real hang in Task 3 — encoding/json matches
// struct fields case-insensitively and a later duplicate wins, so a
// stored "Stream" param would flip the worker into streaming while the
// gateway waits for a single result frame), so case variants are tested
// explicitly alongside the exact-lowercase reserved spellings.
func TestSetAliasReservedParamRejected(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		key  string
	}{
		{"model exact lowercase", "model"},
		{"stream exact lowercase", "stream"},
		{"Model capitalized", "Model"},
		{"MODEL uppercase", "MODEL"},
		{"Stream capitalized", "Stream"},
		{"sTrEaM mixed case", "sTrEaM"},
		// U+017F LATIN SMALL LETTER LONG S folds onto "s" the way
		// encoding/json matches field names, but strings.ToLower leaves it
		// alone — so a ToLower guard admitted this key, and because
		// marshalling sorts keys it then sorted after "stream" and won the
		// worker's stream probe downstream. Must be rejected here too, or a
		// hand-written alias could desync gateway and worker.
		{"long-s stream homoglyph", "\u017ftream"},
		{"long-s STREAM homoglyph", "\u017fTREAM"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newRuntime(t)
			stream := sid(t, "acme-gpt-4")

			res, err := rt.Handle(ctx, stream, setCmd("model-a", map[string]string{tc.key: "x"}), es.Meta{})
			if !errors.Is(err, alias.ErrReservedParam) {
				t.Fatalf("set params {%q: x}: got %v, want ErrReservedParam", tc.key, err)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("err.Error() = %q, want it to name the offending key %q", err.Error(), tc.key)
			}
			if len(res.Events) != 0 {
				t.Fatalf("set reserved param %q: %d events, want 0", tc.key, len(res.Events))
			}
		})
	}
}

// TestSetAliasNonReservedParamAccepted proves the reserved-key rejection
// does not overreach: an ordinary alias param such as "dimensions" (the
// embeddings-specific param this milestone adds) is accepted and stored
// exactly as sent.
func TestSetAliasNonReservedParamAccepted(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme-embed")

	res, err := rt.Handle(ctx, stream, setCmd("text-embedding-3", map[string]string{"dimensions": "512"}), es.Meta{})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("%d events, want 1", len(res.Events))
	}
	if got := res.State.GetParams()["dimensions"]; got != "512" {
		t.Fatalf("state.params[dimensions] = %q, want 512", got)
	}
}
