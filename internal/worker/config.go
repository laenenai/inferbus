package worker

import (
	"fmt"
	"os"

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
	for i := range c.Models {
		if c.Models[i].MaxInflight <= 0 {
			c.Models[i].MaxInflight = 4
		}
	}
	return c, nil
}
