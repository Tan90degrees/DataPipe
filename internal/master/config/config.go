package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"datapipe/internal/common/config"
)

type MasterConfig struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
}

func (c *MasterConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type Config struct {
	Master   MasterConfig `yaml:"master"`
	Database DatabaseConfig `yaml:"database"`
	Redis    RedisConfig `yaml:"redis"`
	Logging  LoggingConfig `yaml:"logging"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.applyDefaults()

	return &cfg, nil
}

func LoadFromCommon(cfg *config.Config) *Config {
	return &Config{
		Master: MasterConfig{
			Host: cfg.GetMasterHost(),
			Port: cfg.GetMasterPort(),
		},
		Database: DatabaseConfig{
			Host:     cfg.GetDatabaseHost(),
			Port:     cfg.GetDatabasePort(),
			Username: cfg.GetDatabaseUsername(),
			Password: cfg.GetDatabasePassword(),
			Database: cfg.GetDatabaseName(),
		},
		Redis: RedisConfig{
			Host:     cfg.GetRedisHost(),
			Port:     cfg.GetRedisPort(),
			Password: cfg.GetRedisPassword(),
			DB:       cfg.GetRedisDB(),
		},
		Logging: LoggingConfig{
			Level:  cfg.GetLogLevel(),
			Format: cfg.GetLogFormat(),
		},
	}
}

func (c *Config) applyDefaults() {
	if c.Master.Port == 0 {
		c.Master.Port = 8080
	}
	if c.Master.Host == "" {
		c.Master.Host = "0.0.0.0"
	}
	if c.Database.Host == "" {
		c.Database.Host = "localhost"
	}
	if c.Database.Port == 0 {
		c.Database.Port = 5432
	}
	if c.Redis.Host == "" {
		c.Redis.Host = "localhost"
	}
	if c.Redis.Port == 0 {
		c.Redis.Port = 6379
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "INFO"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
}
