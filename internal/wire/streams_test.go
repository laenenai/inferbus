package wire_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

func TestEnsureStreamsIdempotent(t *testing.T) {
	_, js := testutil.RunNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ { // second call must not error
		if err := wire.EnsureStreams(ctx, js); err != nil {
			t.Fatalf("EnsureStreams pass %d: %v", i+1, err)
		}
	}

	// Check INFERENCE stream
	info, err := js.Stream(ctx, wire.StreamInference)
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	cfg := info.CachedInfo().Config
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "inference.req.>" {
		t.Fatalf("INFERENCE subjects = %v", cfg.Subjects)
	}
	if cfg.Retention != jetstream.WorkQueuePolicy {
		t.Fatalf("INFERENCE Retention = %v, want WorkQueuePolicy", cfg.Retention)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("INFERENCE Storage = %v, want FileStorage", cfg.Storage)
	}
	if cfg.MaxAge != 30*time.Minute {
		t.Fatalf("INFERENCE MaxAge = %v, want 30*time.Minute", cfg.MaxAge)
	}

	// Check METERING stream
	info, err = js.Stream(ctx, wire.StreamMetering)
	if err != nil {
		t.Fatalf("METERING missing: %v", err)
	}
	cfg = info.CachedInfo().Config
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Fatalf("METERING Retention = %v, want LimitsPolicy", cfg.Retention)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("METERING Storage = %v, want FileStorage", cfg.Storage)
	}
	if cfg.MaxAge != 7*24*time.Hour {
		t.Fatalf("METERING MaxAge = %v, want 7*24*time.Hour", cfg.MaxAge)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "metering.usage.>" {
		t.Fatalf("METERING subjects = %v", cfg.Subjects)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := wire.Message{Kind: wire.KindChunk, Seq: 3, Payload: json.RawMessage(`{"x":1}`)}
	b, _ := json.Marshal(in)
	var out wire.Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Kind != wire.KindChunk || out.Seq != 3 || string(out.Payload) != `{"x":1}` {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestUsageEventRoundTrip(t *testing.T) {
	ts := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	in := wire.UsageEvent{
		ReqID:          "req-123",
		Org:            "org-abc",
		Project:        "proj-xyz",
		KeyID:          "key-999",
		Alias:          "alias-prod",
		Model:          "gpt-4",
		Provider:       "openai",
		Kind:           "chat",
		Status:         "ok",
		ErrorCode:      "err-code-123",
		WorkerID:       "worker-1",
		Usage:          wire.Usage{PromptTokens: 1, CompletionTokens: 2, CachedTokens: 3},
		TTFTMillis:     100,
		DurationMillis: 500,
		QueueMillis:    50,
		Estimated:      true,
		TS:             ts,
	}

	// Marshal to JSON
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Verify key fields are present in JSON with correct snake_case names
	keys := []string{"req_id", "prompt_tokens", "error_code", "ttft_ms", "estimated"}
	for _, key := range keys {
		if !bytes.Contains(b, []byte(`"`+key+`"`)) {
			t.Fatalf("JSON missing key %q: %s", key, string(b))
		}
	}

	// Unmarshal into fresh value
	var out wire.UsageEvent
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Assert all fields survive round trip
	if out.ReqID != in.ReqID {
		t.Errorf("ReqID mismatch: %q != %q", out.ReqID, in.ReqID)
	}
	if out.Org != in.Org {
		t.Errorf("Org mismatch: %q != %q", out.Org, in.Org)
	}
	if out.Project != in.Project {
		t.Errorf("Project mismatch: %q != %q", out.Project, in.Project)
	}
	if out.KeyID != in.KeyID {
		t.Errorf("KeyID mismatch: %q != %q", out.KeyID, in.KeyID)
	}
	if out.Alias != in.Alias {
		t.Errorf("Alias mismatch: %q != %q", out.Alias, in.Alias)
	}
	if out.Model != in.Model {
		t.Errorf("Model mismatch: %q != %q", out.Model, in.Model)
	}
	if out.Provider != in.Provider {
		t.Errorf("Provider mismatch: %q != %q", out.Provider, in.Provider)
	}
	if out.Kind != in.Kind {
		t.Errorf("Kind mismatch: %q != %q", out.Kind, in.Kind)
	}
	if out.Status != in.Status {
		t.Errorf("Status mismatch: %q != %q", out.Status, in.Status)
	}
	if out.ErrorCode != in.ErrorCode {
		t.Errorf("ErrorCode mismatch: %q != %q", out.ErrorCode, in.ErrorCode)
	}
	if out.WorkerID != in.WorkerID {
		t.Errorf("WorkerID mismatch: %q != %q", out.WorkerID, in.WorkerID)
	}
	if out.Usage.PromptTokens != in.Usage.PromptTokens {
		t.Errorf("PromptTokens mismatch: %d != %d", out.Usage.PromptTokens, in.Usage.PromptTokens)
	}
	if out.Usage.CompletionTokens != in.Usage.CompletionTokens {
		t.Errorf("CompletionTokens mismatch: %d != %d", out.Usage.CompletionTokens, in.Usage.CompletionTokens)
	}
	if out.Usage.CachedTokens != in.Usage.CachedTokens {
		t.Errorf("CachedTokens mismatch: %d != %d", out.Usage.CachedTokens, in.Usage.CachedTokens)
	}
	if out.TTFTMillis != in.TTFTMillis {
		t.Errorf("TTFTMillis mismatch: %d != %d", out.TTFTMillis, in.TTFTMillis)
	}
	if out.DurationMillis != in.DurationMillis {
		t.Errorf("DurationMillis mismatch: %d != %d", out.DurationMillis, in.DurationMillis)
	}
	if out.QueueMillis != in.QueueMillis {
		t.Errorf("QueueMillis mismatch: %d != %d", out.QueueMillis, in.QueueMillis)
	}
	if out.Estimated != in.Estimated {
		t.Errorf("Estimated mismatch: %v != %v", out.Estimated, in.Estimated)
	}
	if out.TS != in.TS {
		t.Errorf("TS mismatch: %v != %v", out.TS, in.TS)
	}
}

func TestMessageErrorFrame(t *testing.T) {
	in := wire.Message{
		Kind: wire.KindError,
		Seq:  5,
		Error: &wire.WireError{
			Code:       "RATE_LIMIT",
			Message:    "too many requests",
			HTTPStatus: 429,
		},
	}
	b, _ := json.Marshal(in)
	var out wire.Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Kind != wire.KindError || out.Seq != 5 {
		t.Fatalf("Kind/Seq mismatch: %v/%d", out.Kind, out.Seq)
	}
	if out.Error == nil {
		t.Fatal("Error field nil after unmarshal")
	}
	if out.Error.Code != "RATE_LIMIT" || out.Error.Message != "too many requests" || out.Error.HTTPStatus != 429 {
		t.Fatalf("Error mismatch: %+v", out.Error)
	}
}
