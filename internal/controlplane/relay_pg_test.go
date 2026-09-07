package controlplane_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/controlplane/org"
	"github.com/laenenai/inferbus/internal/testutil"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/postgres"
)

// TestRunRelay_Postgres is finding I4: TestRunRelay_SQLite above exercises
// RunRelay's delivery.Checkpoints/Poller branch (sqlite.Store has a
// gap-free single-writer cursor), but never the delivery.Drainer/Relay
// branch a real controlplane deployment actually runs — postgres.Store's
// gap-safe claim-drain (relay.go's own doc comment: "the Postgres Store.
// Drain satisfies [delivery.Drainer]"). This test drives that branch
// end-to-end against a real Postgres database (named by CP_TEST_PG_DSN,
// skipping cleanly when unset — this suite never starts a Postgres
// container itself) and the real "controlplane" workspace token
// (cmd/inferbus/roles.go's runControlplane uses exactly
// pgStore.Workspace("controlplane")), asserting the delivered subjects are
// the real "evt.controlplane.org.*" shape (not sqlite's "evt.default.*"
// substitution — see TestRunRelay_SQLite's doc comment) and that every
// envelope decodes back to what was appended.
func TestRunRelay_Postgres(t *testing.T) {
	dsn := os.Getenv("CP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("CP_TEST_PG_DSN not set; skipping Postgres relay/Drainer integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgStore, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(pgStore.Close)

	store := pgStore.Workspace("controlplane")

	orgID := fmt.Sprintf("pg-relay-%d", time.Now().UnixNano())
	stream, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgStore.Pool().Exec(context.Background(),
			`DELETE FROM events WHERE workspace_id = $1 AND stream_id = $2`,
			"controlplane", stream.Canonical())
	})

	rt := aggregate.NewRuntime(store, org.Decider, org.Codec())

	cmds := []*controlplanev1.OrgCommand{
		{Kind: &controlplanev1.OrgCommand_Create{
			Create: &controlplanev1.CreateOrg{Id: orgID, Name: "PG Relay Org", OwnerSub: "sub-owner"},
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

	sub, err := nc.SubscribeSync("evt.controlplane.org.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// RunRelay needs the RAW pgStore, not the workspace-scoped store used
	// above for the aggregate runtime: pgStore.Drain implements
	// delivery.Drainer directly, while the workspace-scoped view
	// implements neither delivery.Drainer nor delivery.Checkpoints at all
	// (see RunRelay's doc comment, I4) — this is the exact production
	// wiring cmd/inferbus/roles.go's runControlplane uses.
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- controlplane.RunRelay(ctx, pgStore, js)
	}()

	wantSubjects := []string{
		"evt.controlplane.org.orgcreated",
		"evt.controlplane.org.memberupserted",
		"evt.controlplane.org.projectcreated",
	}

	for i, wantEnv := range want {
		msg, err := sub.NextMsg(10 * time.Second)
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
		if gotEnv.Workspace != "controlplane" {
			t.Fatalf("message %d Workspace = %q, want the real %q token (not sqlite's default substitution)", i, gotEnv.Workspace, "controlplane")
		}
		if string(gotEnv.Payload) != string(wantEnv.Payload) {
			t.Fatalf("message %d Payload mismatch", i)
		}
	}

	// Clean shutdown: cancelling ctx must make RunRelay return promptly
	// (delivery.Relay.Run documents "Returns ctx.Err() on cancellation",
	// same contract as the Poller path TestRunRelay_SQLite already covers).
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
