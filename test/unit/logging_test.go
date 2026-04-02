package unit

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"datapipe/internal/common/logging"
)

func TestLoggerOutput(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	logger.Info("test message")

	output := buf.String()
	if output == "" {
		t.Error("expected log output, got empty string")
	}

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.Level != "INFO" {
		t.Errorf("expected level 'INFO', got '%s'", entry.Level)
	}
	if entry.Message != "test message" {
		t.Errorf("expected message 'test message', got '%s'", entry.Message)
	}
	if entry.Service != "test-service" {
		t.Errorf("expected service 'test-service', got '%s'", entry.Service)
	}
}

func TestStructuredLogging(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	fields := logging.Fields{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}

	logger.Info("structured message", fields)

	output := buf.String()

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.Message != "structured message" {
		t.Errorf("expected message 'structured message', got '%s'", entry.Message)
	}

	if entry.Fields == nil {
		t.Fatal("expected fields in log entry")
	}

	if entry.Fields["key1"] != "value1" {
		t.Errorf("expected key1='value1', got '%v'", entry.Fields["key1"])
	}
	if entry.Fields["key2"] != float64(123) {
		t.Errorf("expected key2=123, got '%v'", entry.Fields["key2"])
	}
}

func TestLogLevels(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)
	logger.SetLevel(logging.DEBUG)

	testCases := []struct {
		level    logging.Level
		funcName string
		msg      string
	}{
		{logging.DEBUG, "Debug", "debug message"},
		{logging.INFO, "Info", "info message"},
		{logging.WARN, "Warn", "warn message"},
		{logging.ERROR, "Error", "error message"},
	}

	for _, tc := range testCases {
		buf.Reset()
		switch tc.funcName {
		case "Debug":
			logger.Debug(tc.msg)
		case "Info":
			logger.Info(tc.msg)
		case "Warn":
			logger.Warn(tc.msg)
		case "Error":
			logger.Error(tc.msg)
		}

		var entry logging.LogEntry
		if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
			t.Fatalf("failed to parse log entry for %s: %v", tc.funcName, err)
		}

		if entry.Level != tc.level.String() {
			t.Errorf("expected level '%s', got '%s'", tc.level.String(), entry.Level)
		}
	}
}

func TestLogLevelFiltering(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)
	logger.SetLevel(logging.WARN)

	logger.Debug("debug message")
	if buf.Len() != 0 {
		t.Error("debug message should be filtered")
	}

	buf.Reset()
	logger.Info("info message")
	if buf.Len() != 0 {
		t.Error("info message should be filtered")
	}

	buf.Reset()
	logger.Warn("warn message")
	if buf.Len() == 0 {
		t.Error("warn message should not be filtered")
	}

	buf.Reset()
	logger.Error("error message")
	if buf.Len() == 0 {
		t.Error("error message should not be filtered")
	}
}

func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	initialFields := logging.Fields{"initial": "value"}
	loggerWithFields := logger.WithFields(initialFields)

	loggerWithFields.Info("message with fields")

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.Fields["initial"] != "value" {
		t.Errorf("expected initial='value', got '%v'", entry.Fields["initial"])
	}
}

func TestLoggerWithTaskID(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	loggerWithTask := logger.WithTaskID("task-123")
	loggerWithTask.Info("task message")

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.TaskID != "task-123" {
		t.Errorf("expected task_id='task-123', got '%s'", entry.TaskID)
	}
}

func TestLoggerWithPipelineID(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	loggerWithPipeline := logger.WithPipelineID("pipeline-456")
	loggerWithPipeline.Info("pipeline message")

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.PipelineID != "pipeline-456" {
		t.Errorf("expected pipeline_id='pipeline-456', got '%s'", entry.PipelineID)
	}
}

func TestLoggerWithNodeID(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	loggerWithNode := logger.WithNodeID("node-789")
	loggerWithNode.Info("node message")

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.NodeID != "node-789" {
		t.Errorf("expected node_id='node-789', got '%s'", entry.NodeID)
	}
}

func TestLogTimestamp(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	beforeTime := time.Now().UTC()
	logger.Info("timestamp test")
	afterTime := time.Now().UTC()

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	parsedTime, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
	if err != nil {
		t.Fatalf("failed to parse timestamp: %v", err)
	}

	if parsedTime.Before(beforeTime) || parsedTime.After(afterTime) {
		t.Errorf("timestamp %v is not between %v and %v", parsedTime, beforeTime, afterTime)
	}
}

func TestLogFormatText(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	textLogger, ok := logger.(*logging.logger)
	if !ok {
		t.Skip("logger is not standard logger type")
	}

	originalFormat := textLogger.format
	textLogger.format = "text"
	defer func() { textLogger.format = originalFormat }()

	logger.Info("text format message")

	output := buf.String()
	if !strings.Contains(output, "[") || !strings.Contains(output, "INFO") {
		t.Error("expected text format output with brackets and level")
	}
}

func TestBufferedLogger(t *testing.T) {
	bufferedLogger := logging.NewBufferedLogger("test-service", 100)

	time.Sleep(10 * time.Millisecond)

	bufferedLogger.Info("buffered message")

	if err := bufferedLogger.Close(); err != nil {
		t.Fatalf("failed to close buffered logger: %v", err)
	}
}

func TestParseLevel(t *testing.T) {
	testCases := []struct {
		input    string
		expected logging.Level
	}{
		{"DEBUG", logging.DEBUG},
		{"debug", logging.DEBUG},
		{"INFO", logging.INFO},
		{"info", logging.INFO},
		{"WARN", logging.WARN},
		{"warn", logging.WARN},
		{"ERROR", logging.ERROR},
		{"error", logging.ERROR},
		{"UNKNOWN", logging.INFO},
		{"", logging.INFO},
	}

	for _, tc := range testCases {
		result := logging.ParseLevel(tc.input)
		if result != tc.expected {
			t.Errorf("ParseLevel(%s) = %v, expected %v", tc.input, result, tc.expected)
		}
	}
}

func TestNewLoggerWithConfig(t *testing.T) {
	logger, err := logging.NewLoggerWithConfig(logging.LoggerConfig{
		Level:      "DEBUG",
		Format:     "json",
		OutputPath: "stdout",
		Service:    "config-test",
	})

	if err != nil {
		t.Fatalf("failed to create logger with config: %v", err)
	}

	if logger.GetLevel() != logging.DEBUG {
		t.Errorf("expected level DEBUG, got %v", logger.GetLevel())
	}

	logger.Info("test message")
}

func TestLoggerContext(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "request_id", "req-123")

	var buf bytes.Buffer
	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	_ = ctx

	logger.Info("context message")

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.Message != "context message" {
		t.Errorf("expected message 'context message', got '%s'", entry.Message)
	}
}

func TestLoggerClose(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	if err := logger.Close(); err != nil {
		t.Errorf("unexpected error closing logger: %v", err)
	}
}

func TestLoggerMultipleFields(t *testing.T) {
	var buf bytes.Buffer

	logger := logging.New("test-service")
	logger = logger.WithWriter(&buf)

	loggerWithTask := logger.WithTaskID("task-1")
	loggerWithPipeline := loggerWithTask.WithPipelineID("pipeline-1")
	loggerWithNode := loggerWithPipeline.WithNodeID("node-1")

	fields := logging.Fields{"custom": "field"}
	loggerWithNode.Info("multi-context message", fields)

	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(buf.String()), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if entry.TaskID != "task-1" {
		t.Errorf("expected task_id='task-1', got '%s'", entry.TaskID)
	}
	if entry.PipelineID != "pipeline-1" {
		t.Errorf("expected pipeline_id='pipeline-1', got '%s'", entry.PipelineID)
	}
	if entry.NodeID != "node-1" {
		t.Errorf("expected node_id='node-1', got '%s'", entry.NodeID)
	}
	if entry.Fields["custom"] != "field" {
		t.Errorf("expected custom='field', got '%v'", entry.Fields["custom"])
	}
}
