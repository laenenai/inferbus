package worker

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ModelConfig struct {
	Name          string `yaml:"name"`
	Engine        string `yaml:"engine"` // "openai_http" | "bifrost"
	URL           string `yaml:"url"`    // openai_http base URL; bifrost optional OpenAI-compatible override
	MaxInflight   int    `yaml:"max_inflight"`
	Provider      string `yaml:"provider"`       // bifrost only
	UpstreamModel string `yaml:"upstream_model"` // bifrost only
	APIKeyEnv     string `yaml:"api_key_env"`    // bifrost only: env var name
}

type Config struct {
	WorkerID string        `yaml:"worker_id"`
	NATSURL  string        `yaml:"nats_url"`
	Models   []ModelConfig `yaml:"models"`

	// AdvertiseEvery/AdvertiseTTL override the MODELS KV heartbeat
	// interval and entry TTL (defaults: wire.ModelsHeartbeat /
	// wire.ModelsTTL). Test-only knobs — never loaded from YAML — kept
	// settable cross-package for e2e tests that need faster timing.
	AdvertiseEvery time.Duration `yaml:"-"`
	AdvertiseTTL   time.Duration `yaml:"-"`

	// AckWait/ConsumerMaxDeliver override the INFERENCE consumer's
	// AckWait (default 30s) and MaxDeliver (default 2) — see RunReady's
	// CreateOrUpdateConsumer call. Same shape and rationale as
	// AdvertiseEvery/AdvertiseTTL above: test-only knobs, never loaded
	// from YAML, kept settable cross-package so an e2e redelivery test
	// doesn't have to wait out the full 30s production AckWait to observe
	// a real redelivery.
	AckWait            time.Duration `yaml:"-"`
	ConsumerMaxDeliver int           `yaml:"-"`
}

// Defaults fills in zero-valued fields that must never be empty at
// runtime: WorkerID (a random "<hostname>-<4 hex>" identity, for the
// zero-config path where the operator hasn't set one) and each model's
// MaxInflight. Safe to call multiple times; only touches fields that are
// still unset.
func (c *Config) Defaults() {
	if c.WorkerID == "" {
		h, _ := os.Hostname()
		if h == "" {
			h = "worker"
		}
		var b [2]byte
		_, _ = rand.Read(b[:])
		c.WorkerID = fmt.Sprintf("%s-%x", h, b)
	}
	for i := range c.Models {
		if c.Models[i].MaxInflight <= 0 {
			c.Models[i].MaxInflight = 4
		}
	}
}

func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(c.Models) == 0 {
		return Config{}, fmt.Errorf("%s: no models configured", path)
	}
	c.Defaults()
	return c, nil
}
