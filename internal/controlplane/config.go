package controlplane

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type OIDCConfig struct {
	Issuer   string `yaml:"issuer"`
	Audience string `yaml:"audience"`
}

type Config struct {
	Addr           string     `yaml:"addr"`
	PostgresDSN    string     `yaml:"postgres_dsn"`
	NATSURL        string     `yaml:"nats_url"`
	OIDC           OIDCConfig `yaml:"oidc"`
	PlatformAdmins []string   `yaml:"platform_admins"`
	BootstrapToken string     `yaml:"bootstrap_token"`
	// ClickhouseDSN optionally enables GET /admin/v1/usage (Task 8): when
	// set, runner.go constructs a CHUsageReader against it; when empty
	// (the default), the usage endpoint reports 501 not_configured. Same
	// DSN shape as internal/harvester's clickhouse_dsn
	// ("clickhouse://host:port/dbname").
	ClickhouseDSN string `yaml:"clickhouse_dsn"`
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
		c.Addr = ":8081"
	}
	if c.PostgresDSN == "" {
		return Config{}, fmt.Errorf("%s: postgres_dsn is required", path)
	}
	return c, nil
}
