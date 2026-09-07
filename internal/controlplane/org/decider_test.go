package org_test

import (
	"context"
	"errors"
	"testing"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/org"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// newRuntime stands up a fresh in-memory sqlite store + org runtime, one per
// test (unique DB name derived from t.Name(), shared cache so the single
// connection keeps it alive for the test's lifetime). Mirrors the
// es-lite examples/counter harness.
func newRuntime(t *testing.T) *aggregate.Runtime[*controlplanev1.Org, *controlplanev1.OrgCommand, *controlplanev1.OrgEvent] {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return aggregate.NewRuntime(store, org.Decider, org.Codec())
}

func sid(t *testing.T, id string) es.StreamID {
	t.Helper()
	s, err := es.NewStreamID(org.StreamType, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return s
}

func createCmd(id, name, ownerSub string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_Create{
		Create: &controlplanev1.CreateOrg{Id: id, Name: name, OwnerSub: ownerSub},
	}}
}

func renameCmd(name string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_Rename{
		Rename: &controlplanev1.RenameOrg{Name: name},
	}}
}

func upsertMemberCmd(sub, role string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_UpsertMember{
		UpsertMember: &controlplanev1.UpsertMember{Sub: sub, Role: role},
	}}
}

func removeMemberCmd(sub string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_RemoveMember{
		RemoveMember: &controlplanev1.RemoveMember{Sub: sub},
	}}
}

func createProjectCmd(id, name string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_CreateProject{
		CreateProject: &controlplanev1.CreateProject{Id: id, Name: name},
	}}
}

func archiveProjectCmd(id string) *controlplanev1.OrgCommand {
	return &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_ArchiveProject{
		ArchiveProject: &controlplanev1.ArchiveProject{Id: id},
	}}
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	res, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if res.State.GetId() != "acme" || res.State.GetName() != "Acme Inc" {
		t.Fatalf("state after create = %+v, want id=acme name=Acme Inc", res.State)
	}
	if got := res.State.GetMembers()["sub-owner"]; got != "owner" {
		t.Fatalf("owner role = %q, want owner", got)
	}
	if res.ToVersion != 1 {
		t.Fatalf("version = %d, want 1", res.ToVersion)
	}

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); !errors.Is(err, org.ErrAlreadyExists) {
		t.Fatalf("create twice: got %v, want ErrAlreadyExists", err)
	}
}

func TestRenameBeforeCreateFails(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, renameCmd("New Name"), es.Meta{}); !errors.Is(err, org.ErrNotFound) {
		t.Fatalf("rename before create: got %v, want ErrNotFound", err)
	}
}

func TestMembers(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Invalid role is rejected.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-x", "superuser"), es.Meta{}); !errors.Is(err, org.ErrInvalidRole) {
		t.Fatalf("invalid role: got %v, want ErrInvalidRole", err)
	}

	// Add a second owner and a viewer.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner2", "owner"), es.Meta{}); err != nil {
		t.Fatalf("upsert owner2: %v", err)
	}
	res, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-viewer", "viewer"), es.Meta{})
	if err != nil {
		t.Fatalf("upsert viewer: %v", err)
	}
	if len(res.State.GetMembers()) != 3 {
		t.Fatalf("members = %v, want 3 entries", res.State.GetMembers())
	}

	// Removing a non-last owner works.
	res, err = rt.Handle(ctx, stream, removeMemberCmd("sub-owner2"), es.Meta{})
	if err != nil {
		t.Fatalf("remove non-last owner: %v", err)
	}
	if _, ok := res.State.GetMembers()["sub-owner2"]; ok {
		t.Fatalf("sub-owner2 still present after removal: %v", res.State.GetMembers())
	}

	// Removing the last owner fails.
	if _, err := rt.Handle(ctx, stream, removeMemberCmd("sub-owner"), es.Meta{}); !errors.Is(err, org.ErrLastOwner) {
		t.Fatalf("remove last owner: got %v, want ErrLastOwner", err)
	}

	// The viewer, not being an owner, can still be removed freely.
	if _, err := rt.Handle(ctx, stream, removeMemberCmd("sub-viewer"), es.Meta{}); err != nil {
		t.Fatalf("remove viewer: %v", err)
	}
}

func TestProjects(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := rt.Handle(ctx, stream, createProjectCmd("proj-1", "Project One"), es.Meta{})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	p, ok := res.State.GetProjects()["proj-1"]
	if !ok || p.GetArchived() {
		t.Fatalf("project after create = %+v, want present and not archived", p)
	}

	res, err = rt.Handle(ctx, stream, archiveProjectCmd("proj-1"), es.Meta{})
	if err != nil {
		t.Fatalf("archive project: %v", err)
	}
	if !res.State.GetProjects()["proj-1"].GetArchived() {
		t.Fatalf("project after archive: archived=false, want true")
	}

	if _, err := rt.Handle(ctx, stream, archiveProjectCmd("no-such-project"), es.Meta{}); !errors.Is(err, org.ErrNoSuchProject) {
		t.Fatalf("archive unknown project: got %v, want ErrNoSuchProject", err)
	}

	// Archiving an already-archived project is a true no-op: zero events,
	// no error, no version bump — never a spurious second ProjectArchived.
	res, err = rt.Handle(ctx, stream, archiveProjectCmd("proj-1"), es.Meta{})
	if err != nil {
		t.Fatalf("archive already-archived project: %v", err)
	}
	if len(res.Events) != 0 {
		t.Fatalf("archive already-archived project: %d events, want 0", len(res.Events))
	}
	if res.FromVersion != res.ToVersion {
		t.Fatalf("archive already-archived project: FromVersion=%d ToVersion=%d, want equal (no append)", res.FromVersion, res.ToVersion)
	}
	if !res.State.GetProjects()["proj-1"].GetArchived() {
		t.Fatalf("project after redundant archive: archived=false, want true")
	}
}

// TestRenameAfterCreate exercises the OrgRenamed success path end to end:
// Decide accepting the rename, Evolve applying it, and the codec round-trip
// through the store — all three are only proven together by a fresh Load
// after a commit.
func TestRenameAfterCreate(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := rt.Handle(ctx, stream, renameCmd("Acme Corp"), es.Meta{})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if res.State.GetName() != "Acme Corp" {
		t.Fatalf("state.Name after rename = %q, want %q", res.State.GetName(), "Acme Corp")
	}

	// Reload from the log (fresh fold, forcing the codec's Decode path)
	// to prove the rename survives encode/decode, not just the in-memory
	// Evolve result from Handle.
	state, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetName() != "Acme Corp" {
		t.Fatalf("reloaded name = %q, want %q", state.GetName(), "Acme Corp")
	}
	if version != 2 {
		t.Fatalf("reloaded version = %d, want 2", version)
	}
}

// TestRemoveNonMemberIsNoOp asserts the controller's binding no-op ruling:
// removing a sub that was never a member emits zero events and no error,
// leaves the folded state unchanged, and stays event-free no matter how
// many times it's repeated.
func TestRemoveNonMemberIsNoOp(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	before, versionBefore, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load before: %v", err)
	}

	for i := 0; i < 2; i++ {
		res, err := rt.Handle(ctx, stream, removeMemberCmd("sub-never-added"), es.Meta{})
		if err != nil {
			t.Fatalf("remove non-member (attempt %d): %v", i, err)
		}
		if len(res.Events) != 0 {
			t.Fatalf("remove non-member (attempt %d): %d events, want 0", i, len(res.Events))
		}
		if res.FromVersion != res.ToVersion {
			t.Fatalf("remove non-member (attempt %d): FromVersion=%d ToVersion=%d, want equal (no append)", i, res.FromVersion, res.ToVersion)
		}
	}

	after, versionAfter, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	if versionAfter != versionBefore {
		t.Fatalf("version after no-op removes = %d, want unchanged %d", versionAfter, versionBefore)
	}
	if len(after.GetMembers()) != len(before.GetMembers()) {
		t.Fatalf("members after no-op removes = %v, want unchanged %v", after.GetMembers(), before.GetMembers())
	}
	for sub, role := range before.GetMembers() {
		if after.GetMembers()[sub] != role {
			t.Fatalf("members[%q] after no-op removes = %q, want unchanged %q", sub, after.GetMembers()[sub], role)
		}
	}
}

// TestUpsertMember_LastOwnerDemotionGuard is finding I2: UpsertMember must
// reject demoting the sole owner to a non-owner role exactly like
// RemoveMember already rejects removing the sole owner outright — an
// upsert-based demotion is otherwise an unguarded bypass of that same
// invariant. Promoting, adding a brand-new member, and re-upserting the
// sole owner AS owner (a true no-op role-wise) must all keep working.
func TestUpsertMember_LastOwnerDemotionGuard(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Demoting the sole owner is rejected.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner", "admin"), es.Meta{}); !errors.Is(err, org.ErrLastOwner) {
		t.Fatalf("demote sole owner: got %v, want ErrLastOwner", err)
	}

	// Re-upserting the sole owner AS owner is a no-op role-wise and stays fine.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner", "owner"), es.Meta{}); err != nil {
		t.Fatalf("re-upsert sole owner as owner: %v", err)
	}

	// Adding a brand-new member (even as a non-owner role) is unaffected.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-viewer", "viewer"), es.Meta{}); err != nil {
		t.Fatalf("upsert new member: %v", err)
	}

	// Promoting an existing non-owner member is unaffected.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-viewer", "admin"), es.Meta{}); err != nil {
		t.Fatalf("promote existing member: %v", err)
	}

	// Add a second owner, then demoting either one individually is fine.
	res, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner2", "owner"), es.Meta{})
	if err != nil {
		t.Fatalf("add second owner: %v", err)
	}
	if res.State.GetMembers()["sub-owner2"] != "owner" {
		t.Fatalf("sub-owner2 role = %q, want owner", res.State.GetMembers()["sub-owner2"])
	}
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner2", "admin"), es.Meta{}); err != nil {
		t.Fatalf("demote one of two owners: %v", err)
	}

	// Now sub-owner is the sole owner again: demoting it is once again
	// rejected.
	if _, err := rt.Handle(ctx, stream, upsertMemberCmd("sub-owner", "viewer"), es.Meta{}); !errors.Is(err, org.ErrLastOwner) {
		t.Fatalf("demote sole owner (again): got %v, want ErrLastOwner", err)
	}
}

// TestCreateProject_NoRevival is finding I3: CreateProject must reject an
// id that already exists in the org, whether or not it has been archived —
// archiving is meant to be a one-way retirement of a project id, not an
// undo-able soft-delete a caller can bypass by simply re-creating.
func TestCreateProject_NoRevival(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	if _, err := rt.Handle(ctx, stream, createCmd("acme", "Acme Inc", "sub-owner"), es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, createProjectCmd("proj-1", "Project One"), es.Meta{}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	// Creating the same id again (still live) is rejected.
	if _, err := rt.Handle(ctx, stream, createProjectCmd("proj-1", "Project One Again"), es.Meta{}); !errors.Is(err, org.ErrAlreadyExists) {
		t.Fatalf("create duplicate live project: got %v, want ErrAlreadyExists", err)
	}

	if _, err := rt.Handle(ctx, stream, archiveProjectCmd("proj-1"), es.Meta{}); err != nil {
		t.Fatalf("archive project: %v", err)
	}

	// Creating the same id after archiving is ALSO rejected — no revival.
	res, err := rt.Handle(ctx, stream, createProjectCmd("proj-1", "Reborn"), es.Meta{})
	if !errors.Is(err, org.ErrAlreadyExists) {
		t.Fatalf("create-after-archive: got %v, want ErrAlreadyExists", err)
	}
	_ = res

	// The project stays archived, unaffected by the rejected attempt.
	state, _, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !state.GetProjects()["proj-1"].GetArchived() {
		t.Fatal("project after rejected re-create: archived=false, want true (unchanged)")
	}
	if state.GetProjects()["proj-1"].GetName() != "Project One" {
		t.Fatalf("project name after rejected re-create = %q, want unchanged %q", state.GetProjects()["proj-1"].GetName(), "Project One")
	}
}

// TestFullFold runs the whole command sequence from the brief and confirms
// Load — a fresh fold from the log, not cached state — reconstructs the
// expected members map and archived project.
func TestFullFold(t *testing.T) {
	ctx := context.Background()
	rt := newRuntime(t)
	stream := sid(t, "acme")

	cmds := []*controlplanev1.OrgCommand{
		createCmd("acme", "Acme Inc", "sub-owner"),
		upsertMemberCmd("sub-owner2", "owner"),
		upsertMemberCmd("sub-admin", "admin"),
		removeMemberCmd("sub-owner2"),
		createProjectCmd("proj-1", "Project One"),
		archiveProjectCmd("proj-1"),
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

	wantMembers := map[string]string{
		"sub-owner": "owner",
		"sub-admin": "admin",
	}
	if len(state.GetMembers()) != len(wantMembers) {
		t.Fatalf("members = %v, want %v", state.GetMembers(), wantMembers)
	}
	for sub, role := range wantMembers {
		if got := state.GetMembers()[sub]; got != role {
			t.Fatalf("members[%q] = %q, want %q", sub, got, role)
		}
	}

	proj, ok := state.GetProjects()["proj-1"]
	if !ok {
		t.Fatalf("project proj-1 missing from folded state: %v", state.GetProjects())
	}
	if !proj.GetArchived() {
		t.Fatalf("folded project archived = false, want true")
	}
}
