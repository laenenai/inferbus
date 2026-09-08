package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/laenenai/inferbus/internal/wire"
)

// modelsResponse is the OpenAI-compatible /v1/models response shape — only
// the fields Discover needs.
type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// Discover builds a worker Config by asking an OpenAI-compatible engine at
// engineURL what models it serves (GET <engineURL>/v1/models), instead of
// requiring an operator to hand-write a worker YAML config per model. Every
// discovered id gets one openai_http ModelConfig pointed back at engineURL,
// with maxInflight applied uniformly.
//
// A discovered id is only usable if it is already NATS-subject-safe as-is
// (wire.Slug(id) == id) — the worker later derives NATS subjects from the
// model name via wire.Slug, so an id that sanitizing would alter is skipped
// (with a slog.Warn) rather than silently served under a different name
// than the engine reports. Discover fails if the engine returns no usable
// models at all.
//
// ctx governs both the HTTP round-trip and how long Discover is willing to
// wait; callers should give it a bounded deadline (zero-config startup
// should not hang indefinitely on a misbehaving engine).
func Discover(ctx context.Context, engineURL string, maxInflight int) (Config, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, engineURL+"/v1/models", nil)
	if err != nil {
		return Config{}, fmt.Errorf("discover %s: %w", engineURL, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Config{}, fmt.Errorf("discover %s: %w", engineURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Config{}, fmt.Errorf("discover %s: engine returned %d", engineURL, resp.StatusCode)
	}
	var parsed modelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return Config{}, fmt.Errorf("discover %s: decode /v1/models: %w", engineURL, err)
	}

	var cfg Config
	for _, m := range parsed.Data {
		if wire.Slug(m.ID) != m.ID {
			slog.Warn("discover: skipping model id not NATS-subject-safe", "engine", engineURL, "id", m.ID)
			continue
		}
		cfg.Models = append(cfg.Models, ModelConfig{
			Name:        m.ID,
			Engine:      "openai_http",
			URL:         engineURL,
			MaxInflight: maxInflight,
		})
	}
	if len(cfg.Models) == 0 {
		return Config{}, fmt.Errorf("discover %s: no usable models found", engineURL)
	}
	cfg.Defaults()
	return cfg, nil
}
