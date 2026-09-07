package gateway

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type KeyConfig struct {
	Key     string   `yaml:"key"`
	Name    string   `yaml:"name"`
	Org     string   `yaml:"org"`
	Project string   `yaml:"project"`
	Allow   []string `yaml:"allow"`
}

type Config struct {
	Addr           string            `yaml:"addr"`
	RequestTimeout time.Duration     `yaml:"request_timeout"`
	Keys           []KeyConfig       `yaml:"keys"`
	Aliases        map[string]string `yaml:"aliases"`
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
	return c, nil
}
