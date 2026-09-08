// Tests for GET /admin/v1/workers (M5 hardening, Task 5).
package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/wire"
)

// putWorkerAd JSON-encodes ad and writes it into the MODELS bucket (created
// on demand) under its own worker id, mirroring what
// internal/worker/advertise.go does in production.
func putWorkerAd(t *testing.T, js jetstream.JetStream, ad wire.WorkerAd) {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: wire.BucketModels})
	if err != nil {
		t.Fatalf("create/adopt MODELS bucket: %v", err)
	}
	b, err := json.Marshal(ad)
	if err != nil {
		t.Fatalf("marshal WorkerAd: %v", err)
	}
	if _, err := kv.Put(context.Background(), ad.WorkerID, b); err != nil {
		t.Fatalf("put worker ad %q: %v", ad.WorkerID, err)
	}
}

// putRawModelsEntry writes a raw (possibly malformed) value directly into
// the MODELS bucket under key, bypassing wire.WorkerAd's own marshaling.
func putRawModelsEntry(t *testing.T, js jetstream.JetStream, key string, value []byte) {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: wire.BucketModels})
	if err != nil {
		t.Fatalf("create/adopt MODELS bucket: %v", err)
	}
	if _, err := kv.Put(context.Background(), key, value); err != nil {
		t.Fatalf("put raw entry %q: %v", key, err)
	}
}

func TestAdmin_ListWorkers_PlatformAdminSeesSortedFleet(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	started := time.Now().UTC().Truncate(time.Second)
	putWorkerAd(t, f.js, wire.WorkerAd{
		WorkerID:  "worker-b",
		Models:    []wire.WorkerAdModel{{Name: "gpt-oss-20b", Engine: "vllm", MaxInflight: 4}},
		StartedAt: started,
		LastSeen:  started,
	})
	putWorkerAd(t, f.js, wire.WorkerAd{
		WorkerID:  "worker-a",
		Models:    []wire.WorkerAdModel{{Name: "qwen3", Engine: "vllm", MaxInflight: 2}},
		StartedAt: started,
		LastSeen:  started,
	})

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/workers", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var got struct {
		Workers []wire.WorkerAd `json:"workers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Workers) != 2 {
		t.Fatalf("workers = %+v, want 2 entries", got.Workers)
	}
	if got.Workers[0].WorkerID != "worker-a" || got.Workers[1].WorkerID != "worker-b" {
		t.Fatalf("workers not sorted by worker_id: %+v", got.Workers)
	}
}

func TestAdmin_ListWorkers_NonPlatformAdminForbidden(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-owner": "org-owner-sub"}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	// Seed an org and make org-owner-sub its owner — /admin/v1/workers is
	// PlatformAdmin-only regardless of any org role the caller holds.
	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "org-owner-sub",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	waitForSQL(t, func() (bool, error) {
		_, _, _, err := f.rs.GetOrg(context.Background(), "acme")
		return err == nil, nil
	})

	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/workers", "jwt-owner", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("org-owner (non-platform-admin) listing workers: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypePermission {
		t.Fatalf("error.type = %q, want %q", typ, errTypePermission)
	}
}

func TestAdmin_ListWorkers_BucketAbsentReturnsEmptyList(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	// No worker has ever put anything into MODELS: the bucket itself has
	// never been created. This must be a 200 empty list, not an error.
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/workers", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"workers":[]}`+"\n" {
		t.Fatalf("body = %q, want %q", got, `{"workers":[]}`+"\n")
	}
}

func TestAdmin_ListWorkers_MalformedEntrySkipped(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	started := time.Now().UTC().Truncate(time.Second)
	putWorkerAd(t, f.js, wire.WorkerAd{
		WorkerID:  "worker-good",
		Models:    []wire.WorkerAdModel{{Name: "qwen3", Engine: "vllm", MaxInflight: 2}},
		StartedAt: started,
		LastSeen:  started,
	})
	putRawModelsEntry(t, f.js, "worker-bad", []byte(`{not valid json`))

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/workers", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Workers []wire.WorkerAd `json:"workers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Workers) != 1 || got.Workers[0].WorkerID != "worker-good" {
		t.Fatalf("workers = %+v, want only worker-good (malformed entry skipped)", got.Workers)
	}
}
