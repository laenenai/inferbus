package wire

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestSlug(t *testing.T) {
	cases := map[string]string{
		"llama-70b":         "llama-70b",
		"Qwen/Qwen2.5-7B":   "qwen-qwen2-5-7b",
		"model with spaces": "model-with-spaces",
		"UPPER_case.v1":     "upper-case-v1",
		"a__b":              "a-b",
		"-model-":           "model",
		"org//model":        "org-model",
	}
	for in, want := range cases {
		if got := Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSubjects(t *testing.T) {
	if got := ReqSubject("Qwen/7B"); got != "inference.req.qwen-7b" {
		t.Errorf("ReqSubject = %q", got)
	}
	if got := RespSubject("abc123"); got != "inference.resp.abc123" {
		t.Errorf("RespSubject = %q", got)
	}
	if got := CancelSubject("abc123"); got != "inference.cancel.abc123" {
		t.Errorf("CancelSubject = %q", got)
	}
	if got := UsageSubject("acme", "prod", "llama-70b"); got != "metering.usage.acme.prod.llama-70b" {
		t.Errorf("UsageSubject = %q", got)
	}
	// org/project are slugged too: an org or project name containing "."
	// would otherwise inject extra NATS subject tokens (subjects are
	// dot-delimited), letting a caller-controlled name corrupt the
	// metering.usage.<org>.<project>.<model> hierarchy.
	if got := UsageSubject("Acme Corp.", "prod/eu", "llama-70b"); got != "metering.usage.acme-corp.prod-eu.llama-70b" {
		t.Errorf("UsageSubject with unslugged org/project = %q", got)
	}
	if got := Durable("llama-70b"); got != "model-llama-70b" {
		t.Errorf("Durable = %q", got)
	}
}

func TestWorkerAdRoundTrip(t *testing.T) {
	// Assert constants exist and have correct values
	if got := BucketModels; got != "MODELS" {
		t.Errorf("BucketModels = %q, want %q", got, "MODELS")
	}
	if got := ModelsTTL; got != 45*time.Second {
		t.Errorf("ModelsTTL = %v, want %v", got, 45*time.Second)
	}
	if got := ModelsHeartbeat; got != 15*time.Second {
		t.Errorf("ModelsHeartbeat = %v, want %v", got, 15*time.Second)
	}
	if got := ModelsTTL; got != 3*ModelsHeartbeat {
		t.Errorf("ModelsTTL = %v, want 3*ModelsHeartbeat = %v", got, 3*ModelsHeartbeat)
	}

	// Create a WorkerAd with two models
	// Note: time.Time loses monotonic clock info when marshaled to JSON,
	// so we use UTC().Round(0) to strip the monotonic clock before creating the original.
	now := time.Now().UTC().Round(0)
	original := WorkerAd{
		WorkerID: "worker-123",
		Models: []WorkerAdModel{
			{
				Name:        "llama-70b",
				Engine:      "ollama",
				MaxInflight: 10,
			},
			{
				Name:        "qwen-7b",
				Engine:      "vllm",
				MaxInflight: 20,
			},
		},
		StartedAt: now,
		LastSeen:  now,
	}

	// Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Unmarshal back
	var unmarshaled WorkerAd
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	// Check equality
	if !reflect.DeepEqual(original, unmarshaled) {
		t.Errorf("RoundTrip failed: original %+v != unmarshaled %+v", original, unmarshaled)
	}
}
