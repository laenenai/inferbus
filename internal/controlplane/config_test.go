package controlplane

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigGolden(t *testing.T) {
	// Path to deploy/controlplane.example.yaml
	examplePath := filepath.Join("..", "..", "deploy", "controlplane.example.yaml")
	cfg, err := LoadConfig(examplePath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Check addr is set
	if cfg.Addr == "" {
		t.Error("Addr should not be empty")
	}
	if cfg.Addr != ":8081" {
		t.Errorf("Addr = %q, want :8081", cfg.Addr)
	}

	// Check postgres_dsn is non-empty
	if cfg.PostgresDSN == "" {
		t.Error("PostgresDSN should not be empty in example config")
	}

	// Check bootstrap_token is set
	if cfg.BootstrapToken == "" {
		t.Error("BootstrapToken should not be empty in example config")
	}
	if cfg.BootstrapToken != "dev_admin_change_me" {
		t.Errorf("BootstrapToken = %q, want dev_admin_change_me", cfg.BootstrapToken)
	}
}

func TestLoadConfigMissingDSN(t *testing.T) {
	// Create a temporary config file without postgres_dsn
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test.yaml")

	// Write a config without postgres_dsn
	content := `
addr: :8081
bootstrap_token: test
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	_, err := LoadConfig(cfgPath)
	if err == nil {
		t.Error("LoadConfig should error when postgres_dsn is missing")
	}
}
