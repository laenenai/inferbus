// Package e2e's M4 milestone acceptance test (Task 9): the same full-loop
// assembly as TestM3FullLoop (control plane + gateway kv IAM + worker with a
// FakeEngine), plus the M4 usage pipeline itself — a Harvester consuming
// METERING into a FakeSink (no ClickHouse: docs/design-usage.md §5's Sink
// seam exists exactly so this test can stay hermetic) and a BudgetLedger
// projecting that usage into the BUDGETS KV bucket the gateway already
// watches for 402 enforcement (internal/gateway/gateway.go's
// budgetExceeded).
//
// Flow: admin creates an org/project/alias/key with a 20-token monthly
// budget -> a chat completion (FakeEngine usage: 10+5=15 tokens) succeeds ->
// poll BUDGETS until the ledger has folded that usage (Used >= 15) -> a
// second chat completion (15 more tokens, pushing month-to-date to 30 > 20)
// -> poll BUDGETS until the ledger flips Exceeded -> the next request after
// that propagates is rejected 402 budget_exhausted -> admin raises the
// budget via PUT .../limits -> poll BUDGETS until Exceeded clears -> a chat
// completion succeeds again.
package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/cpkv"
	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/harvester"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

func TestM4BudgetExhaustion(t *testing.T) {
	const bootstrapToken = "e2e-m4-bootstrap-s3cret"
	cp := startControlPlane(t, bootstrapToken)

	// --- bootstrap: org, project, alias, key with a 20-token monthly budget -

	var orgResp struct {
		ID string `json:"id"`
	}
	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/orgs",
		map[string]any{"id": "acme", "name": "Acme", "owner_sub": "u1"}, &orgResp)
	if orgResp.ID != "acme" {
		t.Fatalf("createOrg: id = %q", orgResp.ID)
	}

	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/orgs/acme/projects",
		map[string]any{"id": "default", "name": "Default"}, nil)

	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPut, "/admin/v1/aliases/acme/fast",
		map[string]any{"target": "m1"}, nil)

	var keyResp struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPost, "/admin/v1/keys",
		map[string]any{
			"org": "acme", "project": "default", "name": "budget-key",
			"allow": []string{"fast"}, "monthly_token_budget": 20,
		}, &keyResp)
	if keyResp.Key == "" || keyResp.ID == "" {
		t.Fatalf("createKey response = %+v", keyResp)
	}

	// --- wait for the KEYS KV projection before starting the gateway/worker -

	keyHash := cpkv.HashKey(keyResp.Key)
	pollUntil(t, 10*time.Second, func() bool {
		kv, err := cp.js.KeyValue(context.Background(), cpkv.BucketKeys)
		if err != nil {
			return false
		}
		_, err = kv.Get(context.Background(), keyHash)
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

	select {
	case <-kviam.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("KVIAM did not become ready")
	}

	// FinalUsage sums to 15 tokens (10 prompt + 5 completion) per request —
	// two requests exceed the key's 20-token monthly budget.
	fakeEngine := &testutil.FakeEngine{
		Chunks:     []string{`{"choices":[{"delta":{"content":"hi"}}]}`},
		FinalUsage: wire.Usage{PromptTokens: 10, CompletionTokens: 5},
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

	// --- harvester + budget ledger, backed by a FakeSink (no ClickHouse) ---

	sink := harvester.NewFakeSink()
	hv := harvester.New(cp.nc, cp.js, sink, harvester.Config{
		BatchMaxInterval: 50 * time.Millisecond,
	})
	// Short refresh so the test doesn't wait out the 10s production default
	// (design-usage.md §5's testing-strategy rationale for a configurable
	// interval).
	ledger := harvester.NewBudgetLedger(cp.js, sink, 200*time.Millisecond)
	hv.OnRow(ledger.AddUsage)

	hvCtx, cancelHV := context.WithCancel(context.Background())
	t.Cleanup(cancelHV)
	hvDone := make(chan error, 1)
	ledgerDone := make(chan error, 1)
	go func() { hvDone <- hv.Run(hvCtx) }()
	go func() { ledgerDone <- ledger.Run(hvCtx) }()
	t.Cleanup(func() {
		cancelHV()
		select {
		case <-hvDone:
		case <-time.After(5 * time.Second):
			t.Fatal("harvester did not stop")
		}
		select {
		case <-ledgerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("budget ledger did not stop")
		}
	})

	// --- chat helpers -------------------------------------------------------

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
	drain := func(resp *http.Response) {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	getBudget := func() (cpkv.BudgetEntry, bool) {
		kv, err := cp.js.KeyValue(context.Background(), cpkv.BucketBudgets)
		if err != nil {
			return cpkv.BudgetEntry{}, false
		}
		e, err := kv.Get(context.Background(), keyResp.ID)
		if err != nil {
			return cpkv.BudgetEntry{}, false
		}
		var entry cpkv.BudgetEntry
		if err := json.Unmarshal(e.Value(), &entry); err != nil {
			return cpkv.BudgetEntry{}, false
		}
		return entry, true
	}

	// --- chat 1: 15 tokens, well under the 20-token budget -> 200 ----------

	resp1 := doChat(keyResp.Key)
	if resp1.StatusCode != http.StatusOK {
		drain(resp1)
		t.Fatalf("chat 1 status = %d, want 200", resp1.StatusCode)
	}
	drain(resp1)

	// --- poll BUDGETS until the ledger has folded chat 1's usage ------------

	pollUntil(t, 15*time.Second, func() bool {
		entry, ok := getBudget()
		return ok && entry.Used >= 15
	})

	// --- chat 2: another 15 tokens, pushing month-to-date to 30 > 20 -------
	// Its own response status is not asserted: BUDGETS propagation to the
	// gateway's KVIAM watch is genuinely asynchronous relative to this
	// request (the whole point of the amendment in design-usage.md), so
	// whether the gateway had already observed Exceeded=true at the moment
	// it authorized this particular request is a race the brief's flow
	// doesn't pin down — only the ledger's eventual BUDGETS state, and the
	// request *after* that propagates, are asserted below.

	drain(doChat(keyResp.Key))

	// --- poll BUDGETS until the ledger flips Exceeded -----------------------

	pollUntil(t, 15*time.Second, func() bool {
		entry, ok := getBudget()
		return ok && entry.Exceeded
	})

	// --- the next request after Exceeded propagates must be denied 402 -----

	pollUntil(t, 10*time.Second, func() bool {
		r := doChat(keyResp.Key)
		defer drain(r)
		if r.StatusCode != http.StatusPaymentRequired {
			return false
		}
		var body struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return false
		}
		return body.Error.Type == "budget_exhausted"
	})

	// --- admin raises the budget; poll until Exceeded clears; chat succeeds

	adminRequest(t, cp.adminSrv, bootstrapToken, http.MethodPut, "/admin/v1/keys/"+keyResp.ID+"/limits",
		map[string]any{"monthly_token_budget": 1000}, nil)

	pollUntil(t, 15*time.Second, func() bool {
		entry, ok := getBudget()
		return ok && !entry.Exceeded
	})

	pollUntil(t, 15*time.Second, func() bool {
		r := doChat(keyResp.Key)
		defer drain(r)
		return r.StatusCode == http.StatusOK
	})
}
