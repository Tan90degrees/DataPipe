package unit

import (
	"os"
	"path/filepath"
	"testing"

	"datapipe/internal/common/config"
)

func TestLoadConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	configContent := `
master:
  host: "127.0.0.1"
  port: 8080
worker:
  host: "0.0.0.0"
  port: 8081
  max_concurrent: 10
  heartbeat_timeout: 30
database:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "datapipe"
  max_open_conns: 100
  max_idle_conns: 10
  conn_max_life: 3600
redis:
  host: "localhost"
  port: 6379
  password: ""
  db: 0
  pool_size: 100
logging:
  level: "DEBUG"
  format: "json"
  output_path: "stdout"
  service: "datapipe-test"
metrics:
  enabled: true
  port: 9090
  path: "/metrics"
pipeline:
  default_timeout: 300
  max_retry_count: 3
  buffer_size: 1000
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := config.New(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.GetMasterHost() != "127.0.0.1" {
		t.Errorf("expected master host '127.0.0.1', got '%s'", cfg.GetMasterHost())
	}
	if cfg.GetMasterPort() != 8080 {
		t.Errorf("expected master port 8080, got %d", cfg.GetMasterPort())
	}

	workerCfg := cfg.GetWorkerConfig()
	if workerCfg.Host != "0.0.0.0" {
		t.Errorf("expected worker host '0.0.0.0', got '%s'", workerCfg.Host)
	}
	if workerCfg.Port != 8081 {
		t.Errorf("expected worker port 8081, got %d", workerCfg.Port)
	}
	if workerCfg.MaxConcurrent != 10 {
		t.Errorf("expected max_concurrent 10, got %d", workerCfg.MaxConcurrent)
	}

	dbCfg := cfg.GetDatabaseConfig()
	if dbCfg.Host != "localhost" {
		t.Errorf("expected database host 'localhost', got '%s'", dbCfg.Host)
	}
	if dbCfg.Port != 3306 {
		t.Errorf("expected database port 3306, got %d", dbCfg.Port)
	}
	if dbCfg.Username != "root" {
		t.Errorf("expected database username 'root', got '%s'", dbCfg.Username)
	}

	redisCfg := cfg.GetRedisConfig()
	if redisCfg.Host != "localhost" {
		t.Errorf("expected redis host 'localhost', got '%s'", redisCfg.Host)
	}
	if redisCfg.Port != 6379 {
		t.Errorf("expected redis port 6379, got %d", redisCfg.Port)
	}

	if cfg.GetLogLevel() != "DEBUG" {
		t.Errorf("expected log level 'DEBUG', got '%s'", cfg.GetLogLevel())
	}
	if cfg.GetLogFormat() != "json" {
		t.Errorf("expected log format 'json', got '%s'", cfg.GetLogFormat())
	}

	if !cfg.IsMetricsEnabled() {
		t.Error("expected metrics to be enabled")
	}
	if cfg.GetMetricsPort() != 9090 {
		t.Errorf("expected metrics port 9090, got %d", cfg.GetMetricsPort())
	}

	if cfg.GetPipelineDefaultTimeout() != 300 {
		t.Errorf("expected default timeout 300, got %d", cfg.GetPipelineDefaultTimeout())
	}
	if cfg.GetPipelineMaxRetryCount() != 3 {
		t.Errorf("expected max retry count 3, got %d", cfg.GetPipelineMaxRetryCount())
	}
}

func TestConfigOverrides(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	configContent := `
master:
  host: "0.0.0.0"
  port: 8080
worker:
  host: "0.0.0.0"
  port: 8081
database:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "datapipe"
redis:
  host: "localhost"
  port: 6379
logging:
  level: "INFO"
  format: "json"
  output_path: "stdout"
  service: "datapipe"
metrics:
  enabled: false
  port: 9090
  path: "/metrics"
pipeline:
  default_timeout: 300
  max_retry_count: 3
  buffer_size: 1000
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	os.Setenv("DATAPIPE_MASTER_HOST", "127.0.0.1")
	os.Setenv("DATAPIPE_MASTER_PORT", "9090")
	os.Setenv("DATAPIPE_LOGGING_LEVEL", "DEBUG")
	os.Setenv("DATAPIPE_METRICS_ENABLED", "true")
	defer func() {
		os.Unsetenv("DATAPIPE_MASTER_HOST")
		os.Unsetenv("DATAPIPE_MASTER_PORT")
		os.Unsetenv("DATAPIPE_LOGGING_LEVEL")
		os.Unsetenv("DATAPIPE_METRICS_ENABLED")
	}()

	cfg, err := config.New(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.GetMasterHost() != "127.0.0.1" {
		t.Errorf("expected master host override '127.0.0.1', got '%s'", cfg.GetMasterHost())
	}
	if cfg.GetMasterPort() != 9090 {
		t.Errorf("expected master port override 9090, got %d", cfg.GetMasterPort())
	}
	if cfg.GetLogLevel() != "DEBUG" {
		t.Errorf("expected log level override 'DEBUG', got '%s'", cfg.GetLogLevel())
	}
	if !cfg.IsMetricsEnabled() {
		t.Error("expected metrics to be enabled via env override")
	}
}

func TestConfigDefaults(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	configContent := `
database:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "datapipe"
redis:
  host: "localhost"
  port: 6379
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := config.New(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.GetMasterPort() != 8080 {
		t.Errorf("expected default master port 8080, got %d", cfg.GetMasterPort())
	}
	if cfg.GetMasterHost() != "0.0.0.0" {
		t.Errorf("expected default master host '0.0.0.0', got '%s'", cfg.GetMasterHost())
	}

	workerCfg := cfg.GetWorkerConfig()
	if workerCfg.Port != 8081 {
		t.Errorf("expected default worker port 8081, got %d", workerCfg.Port)
	}
	if workerCfg.MaxConcurrent != 10 {
		t.Errorf("expected default max_concurrent 10, got %d", workerCfg.MaxConcurrent)
	}
	if workerCfg.HeartbeatTimeout != 30 {
		t.Errorf("expected default heartbeat_timeout 30, got %d", workerCfg.HeartbeatTimeout)
	}

	dbCfg := cfg.GetDatabaseConfig()
	if dbCfg.MaxOpenConns != 100 {
		t.Errorf("expected default max_open_conns 100, got %d", dbCfg.MaxOpenConns)
	}
	if dbCfg.MaxIdleConns != 10 {
		t.Errorf("expected default max_idle_conns 10, got %d", dbCfg.MaxIdleConns)
	}

	redisCfg := cfg.GetRedisConfig()
	if redisCfg.PoolSize != 100 {
		t.Errorf("expected default redis pool_size 100, got %d", redisCfg.PoolSize)
	}

	if cfg.GetLogLevel() != "INFO" {
		t.Errorf("expected default log level 'INFO', got '%s'", cfg.GetLogLevel())
	}
	if cfg.GetLogFormat() != "json" {
		t.Errorf("expected default log format 'json', got '%s'", cfg.GetLogFormat())
	}

	if cfg.GetMetricsPort() != 9090 {
		t.Errorf("expected default metrics port 9090, got %d", cfg.GetMetricsPort())
	}

	if cfg.GetPipelineDefaultTimeout() != 300 {
		t.Errorf("expected default pipeline timeout 300, got %d", cfg.GetPipelineDefaultTimeout())
	}
	if cfg.GetPipelineMaxRetryCount() != 3 {
		t.Errorf("expected default max retry count 3, got %d", cfg.GetPipelineMaxRetryCount())
	}
	if cfg.GetPipelineBufferSize() != 1000 {
		t.Errorf("expected default buffer size 1000, got %d", cfg.GetPipelineBufferSize())
	}
}

func TestConfigReload(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	initialContent := `
master:
  host: "0.0.0.0"
  port: 8080
worker:
  host: "0.0.0.0"
  port: 8081
database:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "datapipe"
redis:
  host: "localhost"
  port: 6379
logging:
  level: "INFO"
  format: "json"
  output_path: "stdout"
  service: "datapipe"
metrics:
  enabled: false
  port: 9090
  path: "/metrics"
pipeline:
  default_timeout: 300
  max_retry_count: 3
  buffer_size: 1000
`

	if err := os.WriteFile(configPath, []byte(initialContent), 0644); err != nil {
		t.Fatalf("failed to write initial config file: %v", err)
	}

	cfg, err := config.New(configPath)
	if err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}

	if cfg.GetMasterPort() != 8080 {
		t.Errorf("expected initial master port 8080, got %d", cfg.GetMasterPort())
	}

	updatedContent := `
master:
  host: "0.0.0.0"
  port: 9090
worker:
  host: "0.0.0.0"
  port: 8081
database:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "datapipe"
redis:
  host: "localhost"
  port: 6379
logging:
  level: "DEBUG"
  format: "json"
  output_path: "stdout"
  service: "datapipe"
metrics:
  enabled: true
  port: 9090
  path: "/metrics"
pipeline:
  default_timeout: 300
  max_retry_count: 3
  buffer_size: 1000
`

	if err := os.WriteFile(configPath, []byte(updatedContent), 0644); err != nil {
		t.Fatalf("failed to write updated config file: %v", err)
	}

	if err := cfg.Reload(); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}

	if cfg.GetMasterPort() != 9090 {
		t.Errorf("expected reloaded master port 9090, got %d", cfg.GetMasterPort())
	}
	if cfg.GetLogLevel() != "DEBUG" {
		t.Errorf("expected reloaded log level 'DEBUG', got '%s'", cfg.GetLogLevel())
	}
	if !cfg.IsMetricsEnabled() {
		t.Error("expected metrics to be enabled after reload")
	}
}

func TestConfigSave(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	cfg := &config.Config{}
	if err := cfg.Load(configPath); err == nil {
		t.Error("expected error when loading non-existent config")
	}
}
