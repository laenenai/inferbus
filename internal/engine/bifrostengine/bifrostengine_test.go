package bifrostengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/testutil"
)

func TestChatStreamThroughBifrost(t *testing.T) {
	srv := testutil.MockOpenAI(t,
		[]string{`{"choices":[{"delta":{"content":"a"}}]}`, `{"choices":[{"delta":{"content":"b"}}]}`},
		`{"prompt_tokens":7,"completion_tokens":2}`)
	e, err := New(Config{Provider: "openai", Model: "gpt-4o-mini", APIKey: "test-key", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	usage, err := e.ChatStream(context.Background(), "any",
		json.RawMessage(`{"model":"any","messages":[{"role":"user","content":"hi"}]}`),
		func(c json.RawMessage) error { got = append(got, string(c)); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("no chunks emitted")
	}
	if usage.PromptTokens != 7 || usage.CompletionTokens != 2 {
		t.Fatalf("usage = %+v", usage)
	}
}

// TestEmbedThroughBifrost exercises Embed end-to-end against a mock
// OpenAI-compatible /v1/embeddings endpoint, mirroring
// TestChatStreamThroughBifrost's approach for Chat/ChatStream.
func TestEmbedThroughBifrost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":5,"total_tokens":5}}`)
	}))
	t.Cleanup(srv.Close)
	e, err := New(Config{Provider: "openai", Model: "text-embedding-3-small", APIKey: "test-key", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	body, usage, err := e.Embed(context.Background(), "any", json.RawMessage(`{"input":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if usage.PromptTokens != 5 {
		t.Fatalf("usage = %+v", usage)
	}
	if usage.CompletionTokens != 0 {
		t.Fatalf("usage.CompletionTokens = %d, want 0", usage.CompletionTokens)
	}
	var parsed struct {
		Object string `json:"object"`
	}
	if json.Unmarshal(body, &parsed) != nil || parsed.Object != "list" {
		t.Fatalf("body = %s", body)
	}
}

// TestToEmbedRequestConvertsInput exercises the OpenAI input (string or
// []string) -> schemas.EmbeddingInput conversion directly, independent of a
// live provider round trip.
func TestToEmbedRequestConvertsInput(t *testing.T) {
	e := &Engine{cfg: Config{Provider: "openai", Model: "text-embedding-3-small"}}

	t.Run("string input", func(t *testing.T) {
		req, err := e.toEmbedRequest(json.RawMessage(`{"input":"hello"}`))
		if err != nil {
			t.Fatal(err)
		}
		if req.Input == nil || req.Input.Text == nil || *req.Input.Text != "hello" {
			t.Fatalf("req.Input = %+v, want Text=\"hello\"", req.Input)
		}
		if req.Provider != "openai" || req.Model != "text-embedding-3-small" {
			t.Fatalf("req.Provider/Model = %s/%s, want overridden from cfg", req.Provider, req.Model)
		}
	})

	t.Run("[]string input", func(t *testing.T) {
		req, err := e.toEmbedRequest(json.RawMessage(`{"input":["a","b"]}`))
		if err != nil {
			t.Fatal(err)
		}
		if req.Input == nil || len(req.Input.Texts) != 2 || req.Input.Texts[0] != "a" || req.Input.Texts[1] != "b" {
			t.Fatalf("req.Input = %+v, want Texts=[a b]", req.Input)
		}
	})

	t.Run("dimensions param carried through", func(t *testing.T) {
		req, err := e.toEmbedRequest(json.RawMessage(`{"input":"hi","dimensions":256}`))
		if err != nil {
			t.Fatal(err)
		}
		if req.Params == nil || req.Params.Dimensions == nil || *req.Params.Dimensions != 256 {
			t.Fatalf("req.Params = %+v, want Dimensions=256", req.Params)
		}
	})

	t.Run("non-object body rejected", func(t *testing.T) {
		_, err := e.toEmbedRequest(json.RawMessage(`[1,2]`))
		var ee *ibengine.Error
		if !errors.As(err, &ee) || ee.HTTPStatus != 400 {
			t.Fatalf("err = %v, want *ibengine.Error 400", err)
		}
	})
}

// TestEmbedUnsupportedOperation asserts an engine configured against a
// provider that cannot embed (per bifrost's own
// providerUtils.NewUnsupportedOperationError signal) surfaces
// ibengine.ErrUnsupported, not a generic mapped error.
func TestEmbedUnsupportedOperation(t *testing.T) {
	code := "unsupported_operation"
	berr := &schemas.BifrostError{Error: &schemas.ErrorField{Message: "embedding is not supported by wafer provider", Code: &code}}
	if !isUnsupportedOperation(berr) {
		t.Fatalf("isUnsupportedOperation(%+v) = false, want true", berr)
	}
	other := &schemas.BifrostError{Error: &schemas.ErrorField{Message: "boom"}}
	if isUnsupportedOperation(other) {
		t.Fatalf("isUnsupportedOperation(%+v) = true, want false", other)
	}
}

// TestMapErrorDirect exercises mapError directly for the status codes it
// special-cases, independent of whatever exact error shape the real bifrost
// HTTP client produces for a given upstream response. mapError is called
// directly since this test file lives in package bifrostengine.
func TestMapErrorDirect(t *testing.T) {
	for _, tc := range []struct {
		status     int
		wantCode   string
		wantStatus int
	}{
		{http.StatusUnauthorized, "upstream_error", http.StatusBadGateway},
		{http.StatusForbidden, "upstream_error", http.StatusBadGateway},
		{http.StatusNotFound, "upstream_error", http.StatusBadGateway},
		{http.StatusTooManyRequests, "bifrost_error", http.StatusTooManyRequests},
		{http.StatusInternalServerError, "bifrost_error", http.StatusInternalServerError},
	} {
		status := tc.status
		berr := &schemas.BifrostError{StatusCode: &status, Error: &schemas.ErrorField{Message: "boom"}}
		err := mapError(berr)
		ee, ok := err.(*ibengine.Error)
		if !ok {
			t.Fatalf("status %d: err = %v, want *ibengine.Error", status, err)
		}
		if ee.Code != tc.wantCode || ee.HTTPStatus != tc.wantStatus {
			t.Fatalf("status %d: mapped = %+v, want Code=%s HTTPStatus=%d", status, ee, tc.wantCode, tc.wantStatus)
		}
	}
}
