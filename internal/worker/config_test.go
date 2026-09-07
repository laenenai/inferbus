package worker_test

import (
	"testing"

	"github.com/infbus/infbus/internal/worker"
)

// TestExampleConfigParses is a golden test against the checked-in
// deploy/worker.example.yaml: it must always parse, and the one active
// model entry (the commented-out bifrost example is intentionally not
// loaded) must keep matching what the README's Configuration section and
// the quickstart describe. A change to either the example file or
// worker.LoadConfig that breaks this contract should fail here rather than
// surface as a confusing quickstart failure.
func TestExampleConfigParses(t *testing.T) {
	cfg, err := worker.LoadConfig("../../deploy/worker.example.yaml")
	if err != nil {
		t.Fatalf("LoadConfig(deploy/worker.example.yaml): %v", err)
	}
	if cfg.WorkerID != "gpu-node-01" {
		t.Errorf("WorkerID = %q, want gpu-node-01", cfg.WorkerID)
	}
	if cfg.NATSURL != "nats://localhost:4222" {
		t.Errorf("NATSURL = %q, want nats://localhost:4222", cfg.NATSURL)
	}
	if len(cfg.Models) != 1 {
		t.Fatalf("Models = %+v, want exactly 1 (the bifrost entry is commented out)", cfg.Models)
	}
	m := cfg.Models[0]
	if m.Name != "llama3.2" || m.Engine != "openai_http" {
		t.Errorf("Models[0] name/engine = %q/%q, want llama3.2/openai_http", m.Name, m.Engine)
	}
	if m.URL != "http://host.docker.internal:11434" {
		t.Errorf("Models[0].URL = %q, want http://host.docker.internal:11434", m.URL)
	}
	if m.MaxInflight != 4 {
		t.Errorf("Models[0].MaxInflight = %d, want 4", m.MaxInflight)
	}
}
