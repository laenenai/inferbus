package openaihttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/testutil"
)

func TestChatStreamCollectsChunksAndUsage(t *testing.T) {
	srv := testutil.MockOpenAI(t,
		[]string{`{"choices":[{"delta":{"content":"a"}}]}`, `{"choices":[{"delta":{"content":"b"}}]}`},
		`{"prompt_tokens":7,"completion_tokens":2}`)
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
	if usage.PromptTokens != 7 || usage.CompletionTokens != 2 {
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
