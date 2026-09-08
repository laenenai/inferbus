package gateway

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type KeyConfig struct {
	Key string `yaml:"key"`
	// ID is the key's stable attribution id (M4 Task 1, design-usage.md
	// §3). It's optional in static config — falls back to Name (below)
	// wherever it's consulted (gateway.go's chatCompletions) — since a
	// static deployment has no separate stream-id concept the way the
	// control plane's apikey aggregate does.
	ID      string   `yaml:"id"`
	Name    string   `yaml:"name"`
	Org     string   `yaml:"org"`
	Project string   `yaml:"project"`
	Allow   []string `yaml:"allow"`
}

// IAMConfig selects where the gateway sources key auth + alias resolution
// from. Mode "static" (the default, and the only mode M2 ever had) uses
// this file's own Keys/Aliases. Mode "kv" reads both live from the
// control plane's ALIASES/KEYS NATS KV buckets instead (see
// internal/gateway/kviam.go) — cmd/inferbus/roles.go is what actually
// constructs the KV provider and wires it in, since that needs a
// jetstream.JetStream handle and a context, neither of which LoadConfig
// has.
type IAMConfig struct {
	Mode string `yaml:"mode"`
}

type Config struct {
	Addr           string            `yaml:"addr"`
	RequestTimeout time.Duration     `yaml:"request_timeout"`
	Keys           []KeyConfig       `yaml:"keys"`
	Aliases        map[string]string `yaml:"aliases"`
	IAM            IAMConfig         `yaml:"iam"`
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
	if c.Addr == "" {
		c.Addr = ":8080"
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Minute
	}
	if c.IAM.Mode == "" {
		c.IAM.Mode = "static"
	}
	return c, nil
}
