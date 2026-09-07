// Package openaihttp adapts any OpenAI-compatible HTTP endpoint
// (vLLM, Ollama, llama.cpp's llama-server, MLX) to engine.Engine.
package openaihttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/wire"
)

type Engine struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string, client *http.Client) *Engine {
	if client == nil {
		client = http.DefaultClient
	}
	return &Engine{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func (e *Engine) post(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		e.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &ibengine.Error{
			Code: "upstream_error", HTTPStatus: resp.StatusCode,
			Message: fmt.Sprintf("engine returned %d: %s", resp.StatusCode, b),
		}
	}
	return resp, nil
}

func (e *Engine) Chat(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	resp, err := e.post(ctx, body)
	if err != nil {
		return nil, wire.Usage{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, wire.Usage{}, err
	}
	return b, usageOf(b), nil
}

func (e *Engine) ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error) {
	forced, err := forceStream(body)
	if err != nil {
		return wire.Usage{}, err
	}
	resp, err := e.post(ctx, forced)
	if err != nil {
		return wire.Usage{}, err
	}
	defer resp.Body.Close()
	var usage wire.Usage
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		if u := usageOf([]byte(data)); u != (wire.Usage{}) {
			usage = u
		}
		if err := emit(json.RawMessage(data)); err != nil {
			return usage, err
		}
	}
	return usage, sc.Err()
}

// forceStream sets "stream":true on the request body.
func forceStream(body json.RawMessage) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, &ibengine.Error{Code: "bad_request", HTTPStatus: 400, Message: "request body is not a JSON object"}
	}
	m["stream"] = json.RawMessage("true")
	return json.Marshal(m)
}

func usageOf(b []byte) wire.Usage {
	var probe struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			PromptTokensDetails struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &probe) != nil || probe.Usage == nil {
		return wire.Usage{}
	}
	return wire.Usage{
		PromptTokens:     probe.Usage.PromptTokens,
		CompletionTokens: probe.Usage.CompletionTokens,
		CachedTokens:     probe.Usage.PromptTokensDetails.CachedTokens,
	}
}
