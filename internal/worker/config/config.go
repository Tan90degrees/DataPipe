package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Worker     WorkerConfig     `yaml:"worker"`
	Master     MasterConfig     `yaml:"master"`
	Database   DatabaseConfig   `yaml:"database"`
	Redis      RedisConfig      `yaml:"redis"`
	Logging    LoggingConfig    `yaml:"logging"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Registry   RegistryConfig   `yaml:"registry"`
	Executor   ExecutorConfig   `yaml:"executor"`
}

type WorkerConfig struct {
	ID              string        `yaml:"id"`
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Name            string        `yaml:"name"`
	MaxConcurrent   int           `yaml:"max_concurrent"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	RegisterTimeout   time.Duration `yaml:"register_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
}

func (c *WorkerConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type MasterConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func (c *MasterConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type DatabaseConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
	ConnMaxLife  int    `yaml:"conn_max_life"`
}

func (c *DatabaseConfig) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.Username, c.Password, c.Database)
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	PoolSize int    `yaml:"pool_size"`
}

func (c *RedisConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	OutputPath string `yaml:"output_path"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
}

type RegistryConfig struct {
	FunctionsDir string `yaml:"functions_dir"`
	AutoLoad     bool   `yaml:"auto_load"`
}

type ExecutorConfig struct {
	WorkerCount   int           `yaml:"worker_count"`
	QueueSize     int           `yaml:"queue_size"`
	MaxRetries    int           `yaml:"max_retries"`
	Timeout       time.Duration `yaml:"timeout"`
	RetryBackoff  time.Duration `yaml:"retry_backoff"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	cfg.applyDefaults()

	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Worker.Host == "" {
		c.Worker.Host = "0.0.0.0"
	}
	if c.Worker.Port == 0 {
		c.Worker.Port = 8081
	}
	if c.Worker.MaxConcurrent == 0 {
		c.Worker.MaxConcurrent = 10
	}
	if c.Worker.HeartbeatInterval == 0 {
		c.Worker.HeartbeatInterval = 30 * time.Second
	}
	if c.Worker.HeartbeatTimeout == 0 {
		c.Worker.HeartbeatTimeout = 90 * time.Second
	}
	if c.Worker.RegisterTimeout == 0 {
		c.Worker.RegisterTimeout = 30 * time.Second
	}
	if c.Worker.ShutdownTimeout == 0 {
		c.Worker.ShutdownTimeout = 10 * time.Second
	}

	if c.Executor.WorkerCount == 0 {
		c.Executor.WorkerCount = 4
	}
	if c.Executor.QueueSize == 0 {
		c.Executor.QueueSize = 1000
	}
	if c.Executor.MaxRetries == 0 {
		c.Executor.MaxRetries = 3
	}
	if c.Executor.Timeout == 0 {
		c.Executor.Timeout = 5 * time.Minute
	}
	if c.Executor.RetryBackoff == 0 {
		c.Executor.RetryBackoff = 1 * time.Second
	}

	if c.Logging.Level == "" {
		c.Logging.Level = "INFO"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
	if c.Logging.OutputPath == "" {
		c.Logging.OutputPath = "stdout"
	}

	if c.Metrics.Port == 0 {
		c.Metrics.Port = 9090
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	if c.Database.MaxOpenConns == 0 {
		c.Database.MaxOpenConns = 100
	}
	if c.Database.MaxIdleConns == 0 {
		c.Database.MaxIdleConns = 10
	}
	if c.Database.ConnMaxLife == 0 {
		c.Database.ConnMaxLife = 3600
	}

	if c.Redis.PoolSize == 0 {
		c.Redis.PoolSize = 100
	}
}

func (c *Config) GetWorkerConfig() WorkerConfig {
	return c.Worker
}

func (c *Config) GetMasterConfig() MasterConfig {
	return c.Master
}

func (c *Config) GetDatabaseConfig() DatabaseConfig {
	return c.Database
}

func (c *Config) GetRedisConfig() RedisConfig {
	return c.Redis
}

func (c *Config) GetLoggingConfig() LoggingConfig {
	return c.Logging
}

func (c *Config) GetMetricsConfig() MetricsConfig {
	return c.Metrics
}

func (c *Config) GetExecutorConfig() ExecutorConfig {
	return c.Executor
}
