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
