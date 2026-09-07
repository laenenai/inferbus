package openaihttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/testutil"
)

func TestChatStreamCollectsChunksAndUsage(t *testing.T) {
	srv := testutil.MockOpenAI(t,
		[]string{`{"choices":[{"delta":{"content":"a"}}]}`, `{"choices":[{"delta":{"content":"b"}}]}`},
		`{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":4}}`)
	e := New(srv.URL, srv.Client())
	var got []string
	usage, err := e.ChatStream(context.Background(), "m",
		json.RawMessage(`{"model":"m","messages":[]}`),
		func(c json.RawMessage) error { got = append(got, string(c)); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 { // 2 content chunks + usage chunk
		t.Fatalf("chunks = %d: %v", len(got), got)
	}
	if usage.PromptTokens != 7 || usage.CompletionTokens != 2 || usage.CachedTokens != 4 {
		t.Fatalf("usage = %+v", usage)
	}
}

func TestChatNonStream(t *testing.T) {
	srv := testutil.MockOpenAI(t, nil, `{"prompt_tokens":3,"completion_tokens":1}`)
	e := New(srv.URL, srv.Client())
	body, usage, err := e.Chat(context.Background(), "m", json.RawMessage(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if usage.PromptTokens != 3 {
		t.Fatalf("usage = %+v", usage)
	}
	var parsed struct{ Object string `json:"object"` }
	if json.Unmarshal(body, &parsed) != nil || parsed.Object != "chat.completion" {
		t.Fatalf("body = %s", body)
	}
}

func TestUpstreamErrorMapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"boom"}`, http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	e := New(srv.URL, srv.Client())
	_, _, err := e.Chat(context.Background(), "m", json.RawMessage(`{}`))
	var ee *ibengine.Error
	if !errors.As(err, &ee) || ee.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("err = %v", err)
	}
}

// TestUpstreamAuthErrorsMappedTo502 covers I12: an upstream 401/403/404 is a
// worker/deployment misconfiguration (bad API key, wrong endpoint, wrong
// model id at the *upstream*), not something the calling client did wrong —
// passing it straight through as our own 401/403/404 would misleadingly
// suggest the caller's infbus credentials or request were at fault. Map it
// to a generic 502 upstream_error instead. 408/429/5xx must keep passing
// through unchanged (that's the caller's own rate limit / retry signal).
func TestUpstreamAuthErrorsMappedTo502(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprintf("%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"error":"nope"}`, status)
			}))
			t.Cleanup(srv.Close)
			e := New(srv.URL, srv.Client())
			_, _, err := e.Chat(context.Background(), "m", json.RawMessage(`{}`))
			var ee *ibengine.Error
			if !errors.As(err, &ee) {
				t.Fatalf("err = %v, want *ibengine.Error", err)
			}
			if ee.HTTPStatus != http.StatusBadGateway || ee.Code != "upstream_error" {
				t.Fatalf("mapped error = %+v, want HTTPStatus=502 Code=upstream_error", ee)
			}
		})
	}
}

func TestForceStreamPreservesFields(t *testing.T) {
	// Test that forceStream preserves non-model fields, adds stream=true,
	// and rewrites model to the concrete name passed in.
	body := json.RawMessage(`{"model":"alias","messages":[{"role":"user","content":"hi"}],"temperature":0.5}`)
	result, err := forceStream(body, "m")
	if err != nil {
		t.Fatalf("forceStream failed: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	// Verify stream is true
	if stream := m["stream"]; stream == nil || string(stream) != "true" {
		t.Fatalf("stream field missing or not true: %v", m["stream"])
	}

	// Verify original fields are preserved
	if m["model"] == nil {
		t.Fatalf("model field missing")
	}
	if m["messages"] == nil {
		t.Fatalf("messages field missing")
	}
	if m["temperature"] == nil {
		t.Fatalf("temperature field missing")
	}

	// Verify field values are correct
	var model string
	json.Unmarshal(m["model"], &model)
	if model != "m" {
		t.Fatalf("model value incorrect: %s", model)
	}

	var temp float64
	json.Unmarshal(m["temperature"], &temp)
	if temp != 0.5 {
		t.Fatalf("temperature value incorrect: %f", temp)
	}

	// forceStream must also ask the upstream for a final usage chunk — an
	// OpenAI-compatible streaming response otherwise carries no usage at
	// all, which is why worker.meter ends up estimating token counts.
	var streamOpts struct {
		IncludeUsage bool `json:"include_usage"`
	}
	if m["stream_options"] == nil {
		t.Fatalf("stream_options field missing")
	}
	if err := json.Unmarshal(m["stream_options"], &streamOpts); err != nil {
		t.Fatalf("stream_options not valid JSON: %v", err)
	}
	if !streamOpts.IncludeUsage {
		t.Fatalf("stream_options.include_usage = false, want true")
	}

	// Test that forceStream rejects non-object bodies
	_, err = forceStream(json.RawMessage(`[1,2]`), "m")
	var ee *ibengine.Error
	if !errors.As(err, &ee) || ee.HTTPStatus != 400 {
		t.Fatalf("expected bad_request error for non-object body, got: %v", err)
	}
}

// TestForceStreamPreservesCallerStreamOptions ensures forceStream does not
// clobber a caller-supplied stream_options (e.g. one that already set
// include_usage to a specific value, or included other fields) with its own.
func TestForceStreamPreservesCallerStreamOptions(t *testing.T) {
	body := json.RawMessage(`{"model":"alias","messages":[],"stream_options":{"include_usage":false,"x":1}}`)
	result, err := forceStream(body, "m")
	if err != nil {
		t.Fatalf("forceStream failed: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(m["stream_options"], &got); err != nil {
		t.Fatalf("stream_options not valid JSON: %v", err)
	}
	if got["include_usage"] != false || got["x"] != float64(1) {
		t.Fatalf("stream_options overwritten: %+v, want caller's original preserved", got)
	}
}

// captureModelServer records the "model" field of the last request body it
// received and replies with a minimal valid OpenAI-shaped response (stream
// or non-stream, based on the request).
func captureModelServer(t *testing.T) (*httptest.Server, *string) {
	t.Helper()
	var lastModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &req)
		lastModel = req.Model
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		fmt.Fprint(w, `{"object":"chat.completion","choices":[{"message":{"content":"ok"}}]}`)
	}))
	t.Cleanup(srv.Close)
	return srv, &lastModel
}

func TestChatRewritesModelToConcreteName(t *testing.T) {
	srv, lastModel := captureModelServer(t)
	e := New(srv.URL, srv.Client())
	// Body carries the client-facing alias "fast"; the engine must rewrite
	// it to the concrete model name ("llama3.2") passed as the method arg
	// before forwarding upstream.
	_, _, err := e.Chat(context.Background(), "llama3.2",
		json.RawMessage(`{"model":"fast","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if *lastModel != "llama3.2" {
		t.Fatalf("upstream received model %q, want %q", *lastModel, "llama3.2")
	}
}

func TestChatStreamRewritesModelToConcreteName(t *testing.T) {
	srv, lastModel := captureModelServer(t)
	e := New(srv.URL, srv.Client())
	_, err := e.ChatStream(context.Background(), "llama3.2",
		json.RawMessage(`{"model":"fast","messages":[]}`),
		func(json.RawMessage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if *lastModel != "llama3.2" {
		t.Fatalf("upstream received model %q, want %q", *lastModel, "llama3.2")
	}
}
