package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/controlplane/org"
	"github.com/laenenai/inferbus/internal/testutil"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/sqlite"
)

// TestRunRelay_SQLite appends 3 org events (create org, add member, create
// project) to a real sqlite es.Store, starts RunRelay against an embedded
// JetStream server, and asserts the 3 messages a plain core-NATS subscriber
// receives on "evt.>" decode (via natsjs.DecodeEnvelope) back to the
// appended envelopes, in order, on the documented subject shape.
//
// Subject workspace token: the brief sketches "evt.controlplane.org.*", but
// per eslite_api_notes.md the es.Envelope.Workspace field is "Empty for
// single-workspace backends (SQLite)" (notes lines ~143-144), and
// natsjs.DefaultSubject documents substituting "default" for an empty
// workspace (notes lines ~482-486, confirmed against the pinned v0.5.0
// source at natsjs/natsjs.go:57-64). sqlite.Open's only Option is
// WithoutAutoMigrate (`go doc github.com/laenenai/es-lite/sqlite`) — there
// is no Workspace option to force a "controlplane" token. The notes win per
// the task brief, so this test asserts the documented "evt.default.org.*"
// shape instead.
func TestRunRelay_SQLite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	rt := aggregate.NewRuntime(store, org.Decider, org.Codec())
	stream, err := es.NewStreamID(org.StreamType, "acme")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	cmds := []*controlplanev1.OrgCommand{
		{Kind: &controlplanev1.OrgCommand_Create{
			Create: &controlplanev1.CreateOrg{Id: "acme", Name: "Acme Inc", OwnerSub: "sub-owner"},
		}},
		{Kind: &controlplanev1.OrgCommand_UpsertMember{
			UpsertMember: &controlplanev1.UpsertMember{Sub: "sub-2", Role: "admin"},
		}},
		{Kind: &controlplanev1.OrgCommand_CreateProject{
			CreateProject: &controlplanev1.CreateProject{Id: "proj-1", Name: "Project One"},
		}},
	}
	for i, c := range cmds {
		if _, err := rt.Handle(ctx, stream, c, es.Meta{}); err != nil {
			t.Fatalf("handle cmd %d (%T): %v", i, c.GetKind(), err)
		}
	}

	// The authoritative record of what was appended, to compare the
	// relayed-and-decoded messages against.
	want, err := store.ReadStream(ctx, stream, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(want) != 3 {
		t.Fatalf("appended %d events, want 3", len(want))
	}

	nc, js := testutil.RunNATS(t)

	if err := controlplane.EnsureControlStream(ctx, js); err != nil {
		t.Fatalf("ensure control stream: %v", err)
	}

	sub, err := nc.SubscribeSync("evt.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- controlplane.RunRelay(ctx, store, js)
	}()

	wantSubjects := []string{
		"evt.default.org.orgcreated",
		"evt.default.org.memberupserted",
		"evt.default.org.projectcreated",
	}

	for i, wantEnv := range want {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("NextMsg %d: %v", i, err)
		}
		if msg.Subject != wantSubjects[i] {
			t.Fatalf("message %d subject = %q, want %q", i, msg.Subject, wantSubjects[i])
		}

		gotEnv, err := natsjs.DecodeEnvelope(msg.Header, msg.Data)
		if err != nil {
			t.Fatalf("DecodeEnvelope %d: %v", i, err)
		}
		if gotEnv.EventID != wantEnv.EventID {
			t.Fatalf("message %d EventID = %v, want %v", i, gotEnv.EventID, wantEnv.EventID)
		}
		if gotEnv.StreamID != wantEnv.StreamID {
			t.Fatalf("message %d StreamID = %v, want %v", i, gotEnv.StreamID, wantEnv.StreamID)
		}
		if gotEnv.Version != wantEnv.Version {
			t.Fatalf("message %d Version = %d, want %d", i, gotEnv.Version, wantEnv.Version)
		}
		if gotEnv.GlobalPosition != wantEnv.GlobalPosition {
			t.Fatalf("message %d GlobalPosition = %d, want %d", i, gotEnv.GlobalPosition, wantEnv.GlobalPosition)
		}
		if gotEnv.TypeURL != wantEnv.TypeURL {
			t.Fatalf("message %d TypeURL = %q, want %q", i, gotEnv.TypeURL, wantEnv.TypeURL)
		}
		if gotEnv.Workspace != wantEnv.Workspace {
			t.Fatalf("message %d Workspace = %q, want %q", i, gotEnv.Workspace, wantEnv.Workspace)
		}
		if string(gotEnv.Payload) != string(wantEnv.Payload) {
			t.Fatalf("message %d Payload mismatch", i)
		}
	}

	// Clean shutdown: cancelling ctx must make RunRelay return promptly
	// (Poller.Run/Relay.Run both document "Returns ctx.Err() on
	// cancellation"), proving no goroutine leak.
	cancel()
	select {
	case err := <-relayDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("RunRelay returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunRelay did not return after ctx cancellation")
	}
}
