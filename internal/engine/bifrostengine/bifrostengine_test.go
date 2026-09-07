package bifrostengine

import (
	"context"
	"encoding/json"
	"net/http"
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
