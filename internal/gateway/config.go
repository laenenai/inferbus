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

// AdmissionConfig turns on per-model backlog admission control: before
// publishing, the gateway compares the model's JetStream consumer backlog
// (NumPending + NumAckPending) against a limit and answers 429 +
// Retry-After instead of adding one more request to a queue nobody is
// draining. It is off by default — MaxBacklog 0 means no checker is built
// at all, so an unconfigured gateway pays exactly zero overhead.
type AdmissionConfig struct {
	// MaxBacklog is the default per-model queue depth at (or above) which
	// new requests are rejected. 0 disables admission control entirely.
	MaxBacklog int `yaml:"max_backlog"`
	// RetryAfterSeconds is the Retry-After value sent with the 429.
	// LoadConfig defaults it to 2 when an admission block is present but
	// leaves this unset.
	RetryAfterSeconds int `yaml:"retry_after_seconds"`
	// Overrides maps a CONCRETE model name (post-alias-resolution, the same
	// name wire.Durable() slugs into a consumer name) to its own limit,
	// replacing MaxBacklog for that model. An override of 0 exempts the
	// model from the check.
	Overrides map[string]int `yaml:"overrides"`
}

type Config struct {
	Addr           string            `yaml:"addr"`
	RequestTimeout time.Duration     `yaml:"request_timeout"`
	Keys           []KeyConfig       `yaml:"keys"`
	Aliases        map[string]string `yaml:"aliases"`
	IAM            IAMConfig         `yaml:"iam"`
	Admission      AdmissionConfig   `yaml:"admission"`
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
	// Only fill the Retry-After default when the operator actually asked
	// for admission control: defaulting it unconditionally would leave a
	// nonzero RetryAfterSeconds sitting in every config that never enables
	// the feature, which reads as "configured" to anyone dumping the config.
	if (c.Admission.MaxBacklog > 0 || len(c.Admission.Overrides) > 0) && c.Admission.RetryAfterSeconds == 0 {
		c.Admission.RetryAfterSeconds = defaultRetryAfterSeconds
	}
	return c, nil
}
