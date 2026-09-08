// Package e2e holds the M3 milestone's full-loop acceptance test (Task 11):
// the control plane's admin API bootstraps an org/project/key/alias, that
// state projects live into the ALIASES/KEYS NATS KV buckets, a gateway
// running in `iam.mode: kv` picks it up, and a real chat-completion request
// flows all the way through to a worker running a FakeEngine — then a key
// disable propagates the same way and the gateway starts rejecting the
// same request with 401.
//
// This test assembles the control plane's components directly (relay + KV
// projectors + SQL projector-fed MemReadStore + admin API with a bootstrap
// token) rather than calling controlplane.NewRunner(cfg, nc, js,
// store).Run(ctx): Run() unconditionally opens a real Postgres connection
// pool for its ReadStore (pgxpool.New(cfg.PostgresDSN) ->
// NewPgReadStore), and this repo has no Postgres test fixture (Postgres
// integration tests are env-gated behind CP_TEST_PG_DSN and skip cleanly
// without it — see internal/controlplane/readmodel_test.go's
// TestPgReadStore_Integration). internal/controlplane/admin_test.go
// already established this same pattern (its adminFixture: "a full live
// stack: sqlite es.Store + embedded NATS + RunRelay + RunSQLProjector
// feeding a MemReadStore, exactly as the task brief specifies") for
// exercising the admin API hermetically; this test adds RunKVProjectors
// (Task 11's own concern, not needed by earlier admin-only tests) and
// carries the same assembly across the package boundary into a gateway +
// worker.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/sqlite"

	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"
	"github.com/laenenai/inferbus/internal/cpkv"
	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// noopVerifier is a controlplane.TokenVerifier that always fails. Every
// admin request in this test authenticates via the bootstrap token, which
// auth.go checks before ever consulting the verifier, so this is never
// actually invoked — it exists only to satisfy NewAuthenticator's
// signature without pulling in real OIDC.
type noopVerifier struct{}

func (noopVerifier) Verify(context.Context, string) (string, error) {
	return "", errors.New("e2e: no OIDC configured")
}

// controlPlaneStack is every in-process piece controlplane.Runner would
// otherwise assemble (see the package doc comment for why this test
// builds it directly instead of via Runner.Run), plus the shared NATS
// connection/JetStream context the gateway and worker attach to as well —
// this is a single embedded NATS server serving every role in the test,
// exactly as the real deployment's one NATS cluster does.
type controlPlaneStack struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	adminSrv *httptest.Server
}

func startControlPlane(t *testing.T, bootstrapToken string) *controlPlaneStack {
	t.Helper()

	nc, js := testutil.RunNATS(t)

	store, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	relayCtx, cancelRelay := context.WithCancel(context.Background())
	if err := controlplane.EnsureControlStream(relayCtx, js); err != nil {
		t.Fatalf("ensure control stream: %v", err)
	}
	relayDone := make(chan error, 1)
	go func() { relayDone <- controlplane.RunRelay(relayCtx, store, js) }()
	t.Cleanup(func() {
		cancelRelay()
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not stop")
		}
	})

	kvCtx, cancelKV := context.WithCancel(context.Background())
	kvDone := make(chan error, 1)
	go func() { kvDone <- controlplane.RunKVProjectors(kvCtx, store, js) }()
	t.Cleanup(func() {
		cancelKV()
		select {
		case <-kvDone:
		case <-time.After(5 * time.Second):
			t.Fatal("KV projectors did not stop")
		}
	})

	rs := controlplane.NewMemReadStore()
	sqlCtx, cancelSQL := context.WithCancel(context.Background())
	sqlDone := make(chan error, 1)
	go func() { sqlDone <- controlplane.RunSQLProjector(sqlCtx, store, js, rs) }()
	t.Cleanup(func() {
		cancelSQL()
		select {
		case <-sqlDone:
		case <-time.After(5 * time.Second):
			t.Fatal("SQL projector did not stop")
		}
	})

	orgRT := aggregate.NewRuntime(store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(store, alias.Decider, alias.Codec())

	cfg := controlplane.Config{BootstrapToken: bootstrapToken}
	admin := controlplane.NewAdmin(
		controlplane.NewAuthenticator(cfg, noopVerifier{}),
		rs, orgRT, keyRT, aliasRT,
		func(context.Context) error { return nil }, // resync: unused by this test
		func() bool { return true },                // healthy: unused by this test
		nil,                                        // usage: unused by this test
	)

	adminSrv := httptest.NewServer(admin.Routes())
	t.Cleanup(adminSrv.Close)

	return &controlPlaneStack{nc: nc, js: js, adminSrv: adminSrv}
}

// adminRequest fires one admin API request with the bootstrap token and
// decodes a JSON response body into out (if non-nil). It fails the test on
// a non-2xx status.
func adminRequest(t *testing.T, srv *httptest.Server, token, method, path string, body, out any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody bytes.Buffer
		_, _ = errBody.ReadFrom(resp.Body)
		t.Fatalf("%s %s: status = %d, body = %s", method, path, resp.StatusCode, errBody.String())
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: decode response: %v", method, path, err)
		}
	}
}

// pollUntil polls check every 20ms until it returns true or timeout
// elapses, failing the test otherwise. Every async wait in this test uses
// this (or an equivalent bounded retry loop against real client-visible
// behavior, like retrying an HTTP request) rather than a bare sleep as the
// sole synchronization — the KV projection and the gateway's own KVIAM
// watch are both genuinely asynchronous relative to the admin API call
// that triggered them.
func pollUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestM3FullLoop is the milestone's acceptance test: admin API bootstrap
// flow -> KV projection -> a chat completion through the gateway's kv IAM
// mode, served by a worker with a FakeEngine -> disable the key via admin
// -> KV projection of the deletion -> the same request now gets 401.
func TestM3FullLoop(t *testing.T) {
	const bootstrapToken = "e2e-bootstrap-s3cret"
	cp := startControlPlane(t, bootstrapToken)

	// --- bootstrap: org, project, alias, key -------------------------------

	var orgResp struct {
		ID string `json:"id"`
	}
	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/orgs",
		map[string]any{"id": "acme", "name": "Acme", "owner_sub": "u1"}, &orgResp)
	if orgResp.ID != "acme" {
		t.Fatalf("createOrg: id = %q", orgResp.ID)
	}

	var projResp struct {
		ID string `json:"id"`
	}
	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/orgs/acme/projects",
		map[string]any{"id": "default", "name": "Default"}, &projResp)

	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPut, "/admin/v1/aliases/acme/fast",
		map[string]any{"target": "m1"}, nil)

	var keyResp struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/keys",
		map[string]any{"org": "acme", "project": "default", "name": "e2e-key", "allow": []string{"fast"}}, &keyResp)
	if keyResp.Key == "" || keyResp.ID == "" {
		t.Fatalf("createKey response = %+v", keyResp)
	}

	// --- wait for KV projection: bounded poll against the real buckets ----

	keyHash := cpkv.HashKey(keyResp.Key)
	pollUntil(t, 10*time.Second, func() bool {
		kv, err := cp.js.KeyValue(context.Background(), cpkv.BucketKeys)
		if err != nil {
			return false
		}
		_, err = kv.Get(context.Background(), keyHash)
		return err == nil
	})
	pollUntil(t, 10*time.Second, func() bool {
		kv, err := cp.js.KeyValue(context.Background(), cpkv.BucketAliases)
		if err != nil {
			return false
		}
		_, err = kv.Get(context.Background(), "acme/fast")
		return err == nil
	})

	// --- gateway (kv mode) + worker with a FakeEngine ----------------------

	gwCtx, cancelGW := context.WithCancel(context.Background())
	t.Cleanup(cancelGW)
	kviam, err := gateway.NewKVIAM(gwCtx, cp.js)
	if err != nil {
		t.Fatalf("NewKVIAM: %v", err)
	}
	gw := gateway.NewWithIAM(cp.nc, cp.js, gateway.Config{
		RequestTimeout: 30 * time.Second,
		IAM:            gateway.IAMConfig{Mode: "kv"},
	}, kviam)
	gwSrv := httptest.NewServer(gw.Routes())
	t.Cleanup(gwSrv.Close)

	// Review ruling I6: NewKVIAM no longer blocks — it starts its watch
	// loops and returns immediately, so the gateway can bind and start
	// serving /readyz right away even before KVIAM has a real snapshot.
	// This test cares about a successful chat completion, which needs a
	// real snapshot, so wait for Ready() explicitly (unit coverage for
	// the pre-ready 503 window itself lives in
	// internal/gateway/kviam_test.go /
	// gateway_test.go rather than being re-proven here).
	select {
	case <-kviam.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("KVIAM did not become ready")
	}

	fakeEngine := &testutil.FakeEngine{
		Chunks:     []string{`{"choices":[{"delta":{"content":"hi"}}]}`},
		FinalUsage: wire.Usage{PromptTokens: 3, CompletionTokens: 1},
	}
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(cancelWorker)
	w := worker.New(cp.nc, cp.js, map[string]ibengine.Engine{"m1": fakeEngine}, worker.Config{
		WorkerID: "w1",
		Models:   []worker.ModelConfig{{Name: "m1", MaxInflight: 2}},
	})
	workerReady := make(chan struct{})
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunReady(workerCtx, workerReady) }()
	t.Cleanup(func() {
		cancelWorker()
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not stop")
		}
	})
	select {
	case <-workerReady:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never became ready")
	}

	// --- the actual chat completion, through the gateway's kv IAM mode -----

	doChat := func(key string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatalf("new chat request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("chat request: %v", err)
		}
		return resp
	}

	// Attribution (M4 Task 1): the request published onto the data plane
	// must carry Ib-Key-Id as the key's stable id (keyResp.ID, the apikey
	// aggregate's stream id), never its display-only Name ("e2e-key") —
	// this is what lets M4's usage/budget attribution survive a key
	// rename. Subscribe before firing the request so no frame can be
	// missed.
	reqSub, err := cp.nc.SubscribeSync("inference.req.>")
	if err != nil {
		t.Fatalf("subscribe inference.req.>: %v", err)
	}
	defer reqSub.Unsubscribe()

	resp := doChat(keyResp.Key)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("chat completion status = %d, want 200", resp.StatusCode)
	}

	reqMsg, err := reqSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected a published inference request: %v", err)
	}
	if got := reqMsg.Header.Get(wire.HdrKeyID); got != keyResp.ID {
		t.Fatalf("Ib-Key-Id = %q, want the key's stable id %q (not its display name %q)", got, keyResp.ID, "e2e-key")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}
	// Review ruling I4: don't just check that a [DONE] eventually shows up
	// — assert the stream actually carries the FakeEngine's own delta
	// content through end to end (the gateway didn't just relay something,
	// it relayed the RIGHT thing), and that no SSE "error" event snuck in
	// ahead of [DONE] (which would mean the request technically "completed"
	// but the worker/engine hit a failure mid-stream — see gateway.go's
	// streamOut, which emits such an event on a mid-stream RemoteError).
	var (
		sawDone      bool
		sawContent   bool
		sawErrorLine string
	)
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimPrefix(sc.Text(), "data: ")
		if line == "" {
			continue
		}
		if line == "[DONE]" {
			sawDone = true
			break
		}
		if strings.Contains(line, `"content":"hi"`) {
			sawContent = true
		}
		var maybeErr struct {
			Error *struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &maybeErr); err == nil && maybeErr.Error != nil {
			sawErrorLine = line
		}
	}
	resp.Body.Close()
	if !sawDone {
		t.Fatal("stream never emitted [DONE]")
	}
	if sawErrorLine != "" {
		t.Fatalf("stream carried an SSE error event before [DONE]: %s", sawErrorLine)
	}
	if !sawContent {
		t.Fatal(`stream never carried the FakeEngine's delta content ("content":"hi")`)
	}

	// --- disable the key via admin, wait for the KV projection of the ------
	// --- deletion, then the same request must now be denied ----------------

	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/keys/"+keyResp.ID+"/disable", nil, nil)

	pollUntil(t, 10*time.Second, func() bool {
		kv, err := cp.js.KeyValue(context.Background(), cpkv.BucketKeys)
		if err != nil {
			return false
		}
		_, err = kv.Get(context.Background(), keyHash)
		return errors.Is(err, jetstream.ErrKeyNotFound)
	})

	// The gateway's own KVIAM watch has its own (bounded, see kviam.go's
	// kvProbeInterval) propagation delay on top of the KV bucket
	// itself already reflecting the deletion, so poll the actual
	// client-visible behavior (repeating the real request) rather than
	// assuming one successful KV Get above means the gateway has caught up
	// too.
	pollUntil(t, 15*time.Second, func() bool {
		r := doChat(keyResp.Key)
		defer r.Body.Close()
		return r.StatusCode == http.StatusUnauthorized
	})
}
