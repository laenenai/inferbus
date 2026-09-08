package harvester

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config controls the harvester's batching behavior. Task 6 extends this
// file with yaml tags and a LoadConfig function; for now it holds just the
// fields the batcher (Task 4) and budget ledger (Task 5) need.
type Config struct {
	// Addr is the HTTP address for healthz/readyz probes (default ":8082").
	Addr string `yaml:"addr"`

	// NATSURL is the URL to connect to NATS (required).
	NATSURL string `yaml:"nats_url"`

	// ClickHouseDSN is the connection string to ClickHouse (required).
	ClickHouseDSN string `yaml:"clickhouse_dsn"`

	// BatchMaxEvents is the number of accumulated events that triggers an
	// immediate flush to the sink, regardless of BatchMaxInterval.
	BatchMaxEvents int `yaml:"batch_max_events"`

	// BatchMaxInterval is the maximum time accumulated events sit
	// unflushed before a flush is forced, regardless of BatchMaxEvents.
	BatchMaxInterval time.Duration `yaml:"batch_max_interval"`

	// BudgetRefreshInterval controls how often the budget ledger (Task 5)
	// republishes dirty BUDGETS KV entries and checks for a month
	// rollover. It is NOT a budget poll interval: budgets are learned from
	// a live watch on the KEYS bucket. Unused by the
	// batcher itself but defined here so Config stays in one place.
	BudgetRefreshInterval time.Duration `yaml:"budget_refresh_interval"`
}

// Default values applied by withDefaults for any zero-valued Config field.
const (
	defaultBatchMaxEvents        = 500
	defaultBatchMaxInterval      = 2 * time.Second
	defaultBudgetRefreshInterval = 10 * time.Second
)

// LoadConfig loads harvester configuration from a YAML file.
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
		c.Addr = ":8082"
	}
	if c.NATSURL == "" {
		return Config{}, fmt.Errorf("%s: nats_url is required", path)
	}
	if c.ClickHouseDSN == "" {
		return Config{}, fmt.Errorf("%s: clickhouse_dsn is required", path)
	}
	return c, nil
}

// withDefaults returns a copy of c with any unset (zero-valued) field
// replaced by its default.
func (c Config) withDefaults() Config {
	if c.BatchMaxEvents <= 0 {
		c.BatchMaxEvents = defaultBatchMaxEvents
	}
	if c.BatchMaxInterval <= 0 {
		c.BatchMaxInterval = defaultBatchMaxInterval
	}
	if c.BudgetRefreshInterval <= 0 {
		c.BudgetRefreshInterval = defaultBudgetRefreshInterval
	}
	return c
}
