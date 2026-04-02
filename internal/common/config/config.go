package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type Config struct {
	mu         sync.RWMutex
	Master     MasterConfig
	Worker     WorkerConfig
	Database   DatabaseConfig
	Redis      RedisConfig
	Logging    LoggingConfig
	Metrics    MetricsConfig
	Pipeline   PipelineConfig
	envPrefix  string
	configPath string
}

type MasterConfig struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
}

func (c *MasterConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type WorkerConfig struct {
	Host             string `yaml:"host" json:"host"`
	Port             int    `yaml:"port" json:"port"`
	MaxConcurrent    int    `yaml:"max_concurrent" json:"max_concurrent"`
	HeartbeatTimeout int    `yaml:"heartbeat_timeout" json:"heartbeat_timeout"`
}

func (c *WorkerConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type DatabaseConfig struct {
	Host         string `yaml:"host" json:"host"`
	Port         int    `yaml:"port" json:"port"`
	Username     string `yaml:"username" json:"username"`
	Password     string `yaml:"password" json:"password"`
	Database     string `yaml:"database" json:"database"`
	MaxOpenConns int    `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns" json:"max_idle_conns"`
	ConnMaxLife  int    `yaml:"conn_max_life" json:"conn_max_life"`
}

func (c *DatabaseConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.Username, c.Password, c.Host, c.Port, c.Database)
}

type RedisConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Password string `yaml:"password" json:"password"`
	DB       int    `yaml:"db" json:"db"`
	PoolSize int    `yaml:"pool_size" json:"pool_size"`
}

func (c *RedisConfig) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type LoggingConfig struct {
	Level      string `yaml:"level" json:"level"`
	Format     string `yaml:"format" json:"format"`
	OutputPath string `yaml:"output_path" json:"output_path"`
	Service    string `yaml:"service" json:"service"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Port    int    `yaml:"port" json:"port"`
	Path    string `yaml:"path" json:"path"`
}

type PipelineConfig struct {
	DefaultTimeout int `yaml:"default_timeout" json:"default_timeout"`
	MaxRetryCount  int `yaml:"max_retry_count" json:"max_retry_count"`
	BufferSize     int `yaml:"buffer_size" json:"buffer_size"`
}

var (
	globalConfig *Config
	configOnce   sync.Once
)

func New(configPath string) (*Config, error) {
	cfg := &Config{
		envPrefix:  "DATAPIPE",
		configPath: configPath,
	}
	if err := cfg.Load(configPath); err != nil {
		return nil, err
	}
	return cfg, nil
}

func Get() *Config {
	if globalConfig == nil {
		configOnce.Do(func() {
			defaultPath := getDefaultConfigPath()
			cfg, err := New(defaultPath)
			if err != nil {
				panic(fmt.Sprintf("failed to load default config: %v", err))
			}
			globalConfig = cfg
		})
	}
	return globalConfig
}

func getDefaultConfigPath() string {
	paths := []string{
		"config.yaml",
		"config.yml",
		"./config/config.yaml",
		"./config/config.yml",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "config.yaml"
}

func (c *Config) Load(configPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	c.applyEnvOverrides()
	c.setDefaults()

	return nil
}

func (c *Config) setDefaults() {
	if c.Master.Port == 0 {
		c.Master.Port = 8080
	}
	if c.Master.Host == "" {
		c.Master.Host = "0.0.0.0"
	}
	if c.Worker.Port == 0 {
		c.Worker.Port = 8081
	}
	if c.Worker.Host == "" {
		c.Worker.Host = "0.0.0.0"
	}
	if c.Worker.MaxConcurrent == 0 {
		c.Worker.MaxConcurrent = 10
	}
	if c.Worker.HeartbeatTimeout == 0 {
		c.Worker.HeartbeatTimeout = 30
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
	if c.Logging.Level == "" {
		c.Logging.Level = "INFO"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
	if c.Logging.OutputPath == "" {
		c.Logging.OutputPath = "stdout"
	}
	if c.Logging.Service == "" {
		c.Logging.Service = "datapipe"
	}
	if c.Metrics.Port == 0 {
		c.Metrics.Port = 9090
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}
	if c.Pipeline.DefaultTimeout == 0 {
		c.Pipeline.DefaultTimeout = 300
	}
	if c.Pipeline.MaxRetryCount == 0 {
		c.Pipeline.MaxRetryCount = 3
	}
	if c.Pipeline.BufferSize == 0 {
		c.Pipeline.BufferSize = 1000
	}
}

func (c *Config) applyEnvOverrides() {
	prefix := c.envPrefix
	v := reflect.ValueOf(c).Elem()

	for i := 0; i < v.NumField(); i++ {
		subConfig := v.Field(i)
		if subConfig.Kind() == reflect.Struct {
			for j := 0; j < subConfig.NumField(); j++ {
				field := subConfig.Field(j)
				if !field.CanSet() {
					continue
				}
				fieldType := subConfig.Type().Field(j)
				envKey := fmt.Sprintf("%s_%s_%s",
					strings.ToUpper(prefix),
					strings.ToUpper(v.Type().Field(i).Name),
					strings.ToUpper(fieldType.Name))

				if envVal := os.Getenv(envKey); envVal != "" {
					switch field.Kind() {
					case reflect.String:
						field.SetString(envVal)
					case reflect.Int, reflect.Int32, reflect.Int64:
						if intVal, err := strconv.Atoi(envVal); err == nil {
							field.SetInt(int64(intVal))
						}
					case reflect.Bool:
						if boolVal, err := strconv.ParseBool(envVal); err == nil {
							field.SetBool(boolVal)
						}
					}
				}
			}
		}
	}
}

func (c *Config) GetMasterConfig() MasterConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Master
}

func (c *Config) GetWorkerConfig() WorkerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Worker
}

func (c *Config) GetDatabaseConfig() DatabaseConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database
}

func (c *Config) GetRedisConfig() RedisConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis
}

func (c *Config) GetLoggingConfig() LoggingConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Logging
}

func (c *Config) GetMetricsConfig() MetricsConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metrics
}

func (c *Config) GetPipelineConfig() PipelineConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Pipeline
}

func (c *Config) GetMasterHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Master.Host
}

func (c *Config) GetMasterPort() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Master.Port
}

func (c *Config) GetWorkerHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Worker.Host
}

func (c *Config) GetWorkerPort() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Worker.Port
}

func (c *Config) GetDatabaseHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database.Host
}

func (c *Config) GetDatabasePort() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database.Port
}

func (c *Config) GetDatabaseUsername() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database.Username
}

func (c *Config) GetDatabasePassword() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database.Password
}

func (c *Config) GetDatabaseName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Database.Database
}

func (c *Config) GetRedisHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.Host
}

func (c *Config) GetRedisPort() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.Port
}

func (c *Config) GetRedisPassword() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.Password
}

func (c *Config) GetRedisDB() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.DB
}

func (c *Config) GetLogLevel() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Logging.Level
}

func (c *Config) GetLogFormat() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Logging.Format
}

func (c *Config) GetLogOutputPath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Logging.OutputPath
}

func (c *Config) GetServiceName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Logging.Service
}

func (c *Config) IsMetricsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metrics.Enabled
}

func (c *Config) GetMetricsPort() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metrics.Port
}

func (c *Config) GetMetricsPath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metrics.Path
}

func (c *Config) GetPipelineDefaultTimeout() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Pipeline.DefaultTimeout
}

func (c *Config) GetPipelineMaxRetryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Pipeline.MaxRetryCount
}

func (c *Config) GetPipelineBufferSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Pipeline.BufferSize
}

func (c *Config) Reload() error {
	return c.Load(c.configPath)
}

func GetConfigDir() string {
	exePath, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exePath)
}

func (c *Config) Save(configPath string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
