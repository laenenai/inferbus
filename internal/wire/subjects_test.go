package wire

import "testing"

func TestSlug(t *testing.T) {
	cases := map[string]string{
		"llama-70b":          "llama-70b",
		"Qwen/Qwen2.5-7B":    "qwen-qwen2-5-7b",
		"model with spaces":  "model-with-spaces",
		"UPPER_case.v1":      "upper-case-v1",
		"a__b":               "a-b",
		"-model-":            "model",
		"org//model":         "org-model",
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
	if got := Durable("llama-70b"); got != "model-llama-70b" {
		t.Errorf("Durable = %q", got)
	}
}
