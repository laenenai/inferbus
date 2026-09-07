package bifrostengine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/infbus/infbus/internal/testutil"
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
