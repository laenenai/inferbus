package testutil

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
)

func TestMockOpenAIStreams(t *testing.T) {
	srv := MockOpenAI(t,
		[]string{`{"choices":[{"delta":{"content":"hel"}}]}`, `{"choices":[{"delta":{"content":"lo"}}]}`},
		`{"prompt_tokens":5,"completion_tokens":2}`)
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var dataLines []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(sc.Text(), "data: "))
		}
	}
	if len(dataLines) != 4 { // 2 chunks + usage chunk + [DONE]
		t.Fatalf("got %d data lines: %v", len(dataLines), dataLines)
	}
	if dataLines[len(dataLines)-1] != "[DONE]" {
		t.Fatalf("last line %q", dataLines[len(dataLines)-1])
	}
	if !strings.Contains(dataLines[2], `"usage"`) {
		t.Fatalf("penultimate line missing usage: %q", dataLines[2])
	}
}
