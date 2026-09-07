// Package bifrostengine adapts the embedded Bifrost router
// (github.com/maximhq/bifrost/core) to engine.Engine, giving workers
// multi-provider routing (OpenAI, Anthropic, Ollama, vLLM, ...).
package bifrostengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/wire"
)

type Config struct {
	Provider string // bifrost provider id: "openai", "anthropic", "ollama", ...
	Model    string // upstream model id
	APIKey   string
	BaseURL  string // optional: OpenAI-compatible endpoint override
}

type Engine struct {
	b   *bifrost.Bifrost
	cfg Config
}

func New(cfg Config) (*Engine, error) {
	b, err := bifrost.Init(context.Background(), buildConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("bifrost init: %w", err)
	}
	return &Engine{b: b, cfg: cfg}, nil
}

// staticAccount is the minimal schemas.Account implementation for a
// single-provider, single-key worker adapter: one provider id, one API
// key (allowed for any model), one optional network override (used to
// point Bifrost's OpenAI-compatible providers at a local/mock endpoint).
type staticAccount struct {
	provider schemas.ModelProvider
	key      schemas.Key
	network  schemas.NetworkConfig
}

func (a *staticAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	return []schemas.ModelProvider{a.provider}, nil
}

func (a *staticAccount) GetKeysForProvider(ctx context.Context, providerKey schemas.ModelProvider) ([]schemas.Key, error) {
	return []schemas.Key{a.key}, nil
}

func (a *staticAccount) GetConfigForProvider(providerKey schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	return &schemas.ProviderConfig{NetworkConfig: a.network}, nil
}

// buildConfig maps our Config onto schemas.BifrostConfig. schemas.Account
// is an interface (GetConfiguredProviders / GetKeysForProvider /
// GetConfigForProvider) with no built-in single-provider implementation in
// the SDK, so staticAccount above supplies one: one provider, one API key
// (schemas.Key.Value is a schemas.SecretVar, models whitelisted with "*"),
// and NetworkConfig.BaseURL carrying the optional endpoint override
// (verified via `go doc .../schemas NetworkConfig`: "BaseURL is supported
// for OpenAI, Anthropic, Cohere, Mistral, and Ollama providers").
func buildConfig(cfg Config) schemas.BifrostConfig {
	return schemas.BifrostConfig{
		Account: &staticAccount{
			provider: schemas.ModelProvider(cfg.Provider),
			key: schemas.Key{
				Value:  *schemas.NewSecretVar(cfg.APIKey),
				Models: schemas.WhiteList{"*"},
			},
			network: schemas.NetworkConfig{BaseURL: cfg.BaseURL},
		},
	}
}

// requestBody decodes just the "messages" field of the caller's
// OpenAI-shaped body; schemas.ChatMessage has custom (Un)MarshalJSON that
// speaks the OpenAI wire format directly.
type requestBody struct {
	Messages []schemas.ChatMessage `json:"messages"`
}

// toRequest decodes the OpenAI-shaped body's messages/params into the
// Bifrost request type, overriding provider+model from e.cfg. Per the
// Task 11 carried semantic, the upstream request always uses e.cfg.Model
// (the concrete/upstream model) and never the body's own "model" field
// (the caller-facing alias).
func (e *Engine) toRequest(body json.RawMessage) (*schemas.BifrostChatRequest, error) {
	var rb requestBody
	if err := json.Unmarshal(body, &rb); err != nil {
		return nil, &ibengine.Error{Code: "bad_request", HTTPStatus: 400, Message: "request body is not a JSON object"}
	}
	var params schemas.ChatParameters
	if err := json.Unmarshal(body, &params); err != nil {
		return nil, &ibengine.Error{Code: "bad_request", HTTPStatus: 400, Message: "request body has invalid params"}
	}
	return &schemas.BifrostChatRequest{
		Provider: schemas.ModelProvider(e.cfg.Provider),
		Model:    e.cfg.Model,
		Input:    rb.Messages,
		Params:   &params,
	}, nil
}

func (e *Engine) Chat(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	req, err := e.toRequest(body)
	if err != nil {
		return nil, wire.Usage{}, err
	}
	bctx := schemas.NewBifrostContext(ctx, time.Time{})
	resp, berr := e.b.ChatCompletionRequest(bctx, req)
	if berr != nil {
		return nil, wire.Usage{}, mapError(berr)
	}
	out, err := json.Marshal(resp) // BifrostChatResponse is OpenAI-shaped
	return out, usageOf(resp), err
}

func (e *Engine) ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error) {
	req, err := e.toRequest(body)
	if err != nil {
		return wire.Usage{}, err
	}
	bctx := schemas.NewBifrostContext(ctx, time.Time{})
	ch, berr := e.b.ChatCompletionStreamRequest(bctx, req)
	if berr != nil {
		return wire.Usage{}, mapError(berr)
	}
	var usage wire.Usage
	for chunk := range ch {
		if chunk.BifrostError != nil {
			return usage, mapError(chunk.BifrostError)
		}
		b, err := json.Marshal(chunk) // BifrostStreamChunk is OpenAI-chunk-shaped
		if err != nil {
			return usage, err
		}
		if u := chunkUsage(chunk); u != (wire.Usage{}) {
			usage = u
		}
		if err := emit(b); err != nil {
			return usage, err
		}
	}
	return usage, nil
}

// usageOf reads token usage off a BifrostChatResponse
// (schemas.BifrostLLMUsage: PromptTokens, CompletionTokens,
// PromptTokensDetails.CachedReadTokens).
func usageOf(resp *schemas.BifrostChatResponse) wire.Usage {
	if resp == nil || resp.Usage == nil {
		return wire.Usage{}
	}
	u := wire.Usage{
		PromptTokens:     resp.Usage.PromptTokens,
		CompletionTokens: resp.Usage.CompletionTokens,
	}
	if resp.Usage.PromptTokensDetails != nil {
		u.CachedTokens = resp.Usage.PromptTokensDetails.CachedReadTokens
	}
	return u
}

// chunkUsage reads usage off a stream chunk's embedded *BifrostChatResponse.
// BifrostStreamChunk embeds several response types that each declare their
// own "Usage" field (BifrostTextCompletionResponse, BifrostSpeechStreamResponse,
// BifrostTranscriptionStreamResponse, BifrostImageGenerationStreamResponse),
// so the promoted "chunk.Usage" selector is ambiguous; the embedded
// BifrostChatResponse field must be addressed explicitly.
func chunkUsage(chunk *schemas.BifrostStreamChunk) wire.Usage {
	if chunk == nil || chunk.BifrostChatResponse == nil {
		return wire.Usage{}
	}
	return usageOf(chunk.BifrostChatResponse)
}

func mapError(berr *schemas.BifrostError) error {
	status := 502
	if berr.StatusCode != nil {
		status = *berr.StatusCode
	}
	msg := ""
	if berr.Error != nil {
		msg = berr.Error.Message
	}
	// A 401/403/404 from the upstream provider almost always means this
	// worker's provider config is wrong (bad API key, wrong upstream model
	// id) — not something the calling client did. Map it to a generic 502
	// upstream_error, same as internal/engine/openaihttp, so it doesn't
	// misleadingly point the caller at their own infbus credentials or
	// request. 408/429/5xx pass through unchanged.
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound {
		return &ibengine.Error{Code: "upstream_error", Message: msg, HTTPStatus: http.StatusBadGateway}
	}
	return &ibengine.Error{Code: "bifrost_error", Message: msg, HTTPStatus: status}
}
