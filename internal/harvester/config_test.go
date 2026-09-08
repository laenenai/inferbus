package harvester

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Load the example config from deploy/harvester.example.yaml.
	// This test uses the deployed example file as a golden config.
	examplePath := filepath.Join("..", "..", "deploy", "harvester.example.yaml")
	cfg, err := LoadConfig(examplePath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify addr default/value.
	if cfg.Addr != ":8082" {
		t.Fatalf("addr = %q, want \":8082\"", cfg.Addr)
	}

	// Verify required fields are non-empty.
	if cfg.NATSURL == "" {
		t.Fatal("nats_url is empty; should be required")
	}
	if cfg.ClickHouseDSN == "" {
		t.Fatal("clickhouse_dsn is empty; should be required")
	}

	// Verify batch/refresh values parse correctly.
	if cfg.BatchMaxEvents <= 0 {
		t.Fatalf("batch_max_events = %d, want > 0", cfg.BatchMaxEvents)
	}
	if cfg.BatchMaxInterval <= 0 {
		t.Fatalf("batch_max_interval = %v, want > 0", cfg.BatchMaxInterval)
	}
	if cfg.BudgetRefreshInterval <= 0 {
		t.Fatalf("budget_refresh_interval = %v, want > 0", cfg.BudgetRefreshInterval)
	}

	// Spot-check the expected values from the example file.
	if cfg.BatchMaxEvents != 500 {
		t.Fatalf("batch_max_events = %d, want 500", cfg.BatchMaxEvents)
	}
	if cfg.BatchMaxInterval != 2*time.Second {
		t.Fatalf("batch_max_interval = %v, want 2s", cfg.BatchMaxInterval)
	}
	if cfg.BudgetRefreshInterval != 10*time.Second {
		t.Fatalf("budget_refresh_interval = %v, want 10s", cfg.BudgetRefreshInterval)
	}
}

func TestLoadConfigMissingClickHouseDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := `
addr: ":8082"
nats_url: "nats://localhost:4222"
batch_max_events: 500
batch_max_interval: "2s"
budget_refresh_interval: "10s"
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("LoadConfig should fail when clickhouse_dsn is missing")
	}
	if !strings.Contains(err.Error(), "clickhouse_dsn") {
		t.Fatalf("error message %q should mention clickhouse_dsn", err.Error())
	}
}

func TestLoadConfigMissingNATSURL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := `
addr: ":8082"
clickhouse_dsn: "clickhouse://localhost:9000/inferbus"
batch_max_events: 500
batch_max_interval: "2s"
budget_refresh_interval: "10s"
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("LoadConfig should fail when nats_url is missing")
	}
	if !strings.Contains(err.Error(), "nats_url") {
		t.Fatalf("error message %q should mention nats_url", err.Error())
	}
}
