package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func ParseLevel(s string) LogLevel {
	switch s {
	case "DEBUG", "debug":
		return DEBUG
	case "INFO", "info":
		return INFO
	case "WARN", "warn", "WARNING", "warning":
		return WARN
	case "ERROR", "error":
		return ERROR
	case "FATAL", "fatal":
		return FATAL
	default:
		return INFO
	}
}

type LogEntry struct {
	Timestamp  string                 `json:"timestamp"`
	Level      string                 `json:"level"`
	Service    string                 `json:"service,omitempty"`
	TaskID     string                 `json:"task_id,omitempty"`
	PipelineID string                 `json:"pipeline_id,omitempty"`
	NodeID     string                 `json:"node_id,omitempty"`
	Message    string                 `json:"message"`
	Caller     string                 `json:"caller,omitempty"`
	Context    map[string]interface{} `json:"context,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Duration   float64                `json:"duration_ms,omitempty"`
}

type StructuredLogger interface {
	Debug(msg string, fields ...map[string]interface{})
	Info(msg string, fields ...map[string]interface{})
	Warn(msg string, fields ...map[string]interface{})
	Error(msg string, fields ...map[string]interface{})
	Fatal(msg string, fields ...map[string]interface{})
	WithFields(fields map[string]interface{}) StructuredLogger
	WithTaskID(taskID string) StructuredLogger
	WithPipelineID(pipelineID string) StructuredLogger
	WithNodeID(nodeID string) StructuredLogger
	WithService(service string) StructuredLogger
	SetLevel(level LogLevel)
	GetLevel() LogLevel
	Close() error
}

type structuredLogger struct {
	mu         sync.RWMutex
	level      LogLevel
	format     string
	outputPath string
	service    string
	taskID     string
	pipelineID string
	nodeID     string
	writer     io.Writer
	context    map[string]interface{}
	closeFunc  func() error
	formatter  LogFormatter
}

type LogFormatter interface {
	Format(entry *LogEntry) ([]byte, error)
}

type JSONFormatter struct{}

func (f *JSONFormatter) Format(entry *LogEntry) ([]byte, error) {
	return json.Marshal(entry)
}

type TextFormatter struct {
	Template string
}

func (f *TextFormatter) Format(entry *LogEntry) ([]byte, error) {
	if f.Template == "" {
		f.Template = "[{timestamp}] {level} | {service} | {message}"
	}

	output := f.Template
	output = replacePlaceholder(output, "timestamp", entry.Timestamp)
	output = replacePlaceholder(output, "level", entry.Level)
	output = replacePlaceholder(output, "service", entry.Service)
	output = replacePlaceholder(output, "message", entry.Message)
	output = replacePlaceholder(output, "task_id", entry.TaskID)
	output = replacePlaceholder(output, "pipeline_id", entry.PipelineID)
	output = replacePlaceholder(output, "node_id", entry.NodeID)
	output = replacePlaceholder(output, "caller", entry.Caller)

	if len(entry.Context) > 0 {
		ctxJSON, _ := json.Marshal(entry.Context)
		output += fmt.Sprintf(" | context=%s", string(ctxJSON))
	}

	return []byte(output), nil
}

func replacePlaceholder(template, key, value string) string {
	return fmt.Sprintf("{%s}", key) + "" + template
}

var (
	defaultStructuredLogger *structuredLogger
	loggerOnce              sync.Once
)

func NewStructuredLogger(service string) StructuredLogger {
	return newStructuredLogger(service, "json", "stdout", INFO)
}

func newStructuredLogger(service, format, outputPath string, level LogLevel) *structuredLogger {
	var formatter LogFormatter
	if format == "json" {
		formatter = &JSONFormatter{}
	} else {
		formatter = &TextFormatter{}
	}

	l := &structuredLogger{
		level:      level,
		format:     format,
		outputPath: outputPath,
		service:    service,
		writer:     os.Stdout,
		context:    make(map[string]interface{}),
		formatter:  formatter,
	}

	if outputPath != "stdout" && outputPath != "stderr" {
		dir := filepath.Dir(outputPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		} else {
			file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				l.writer = file
				l.closeFunc = file.Close
			}
		}
	}

	return l
}

func GetStructuredLogger() StructuredLogger {
	if defaultStructuredLogger == nil {
		loggerOnce.Do(func() {
			defaultStructuredLogger = newStructuredLogger("datapipe", "json", "stdout", INFO)
		})
	}
	return defaultStructuredLogger
}

func (l *structuredLogger) output(level LogLevel, msg string, fields ...map[string]interface{}) {
	if level < l.level {
		return
	}

	l.mu.RLock()
	entry := &LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Level:      level.String(),
		Service:    l.service,
		TaskID:     l.taskID,
		PipelineID: l.pipelineID,
		NodeID:     l.nodeID,
		Message:    msg,
		Caller:     l.getCaller(),
	}

	if len(fields) > 0 && fields[0] != nil {
		entry.Context = fields[0]
	} else if len(l.context) > 0 {
		entry.Context = l.context
	}
	l.mu.RUnlock()

	l.writeEntry(entry)

	if level == FATAL {
		l.Close()
		os.Exit(1)
	}
}

func (l *structuredLogger) getCaller() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

func (l *structuredLogger) writeEntry(entry *LogEntry) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	data, err := l.formatter.Format(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to format log entry: %v\n", err)
		return
	}

	l.writer.Write(data)
	l.writer.Write([]byte("\n"))
}

func (l *structuredLogger) Debug(msg string, fields ...map[string]interface{}) {
	l.output(DEBUG, msg, fields...)
}

func (l *structuredLogger) Info(msg string, fields ...map[string]interface{}) {
	l.output(INFO, msg, fields...)
}

func (l *structuredLogger) Warn(msg string, fields ...map[string]interface{}) {
	l.output(WARN, msg, fields...)
}

func (l *structuredLogger) Error(msg string, fields ...map[string]interface{}) {
	l.output(ERROR, msg, fields...)
}

func (l *structuredLogger) Fatal(msg string, fields ...map[string]interface{}) {
	l.output(FATAL, msg, fields...)
}

func (l *structuredLogger) WithFields(fields map[string]interface{}) StructuredLogger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := &structuredLogger{
		level:      l.level,
		format:     l.format,
		outputPath: l.outputPath,
		service:    l.service,
		taskID:     l.taskID,
		pipelineID: l.pipelineID,
		nodeID:     l.nodeID,
		writer:     l.writer,
		context:    make(map[string]interface{}),
		formatter:  l.formatter,
	}

	for k, v := range l.context {
		newLogger.context[k] = v
	}
	for k, v := range fields {
		newLogger.context[k] = v
	}

	return newLogger
}

func (l *structuredLogger) WithTaskID(taskID string) StructuredLogger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.taskID = taskID
	newLogger.context = make(map[string]interface{})
	for k, v := range l.context {
		newLogger.context[k] = v
	}
	return &newLogger
}

func (l *structuredLogger) WithPipelineID(pipelineID string) StructuredLogger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.pipelineID = pipelineID
	newLogger.context = make(map[string]interface{})
	for k, v := range l.context {
		newLogger.context[k] = v
	}
	return &newLogger
}

func (l *structuredLogger) WithNodeID(nodeID string) StructuredLogger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.nodeID = nodeID
	newLogger.context = make(map[string]interface{})
	for k, v := range l.context {
		newLogger.context[k] = v
	}
	return &newLogger
}

func (l *structuredLogger) WithService(service string) StructuredLogger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.service = service
	newLogger.context = make(map[string]interface{})
	for k, v := range l.context {
		newLogger.context[k] = v
	}
	return &newLogger
}

func (l *structuredLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *structuredLogger) GetLevel() LogLevel {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

func (l *structuredLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closeFunc != nil {
		return l.closeFunc()
	}

	if closer, ok := l.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type StructuredLoggerConfig struct {
	Level      string
	Format     string
	OutputPath string
	Service    string
	MaxSize    int64
	MaxBackups int
	MaxAge     int
	Compress   bool
}

func NewStructuredLoggerWithConfig(cfg StructuredLoggerConfig) (StructuredLogger, error) {
	level := ParseLevel(cfg.Level)
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	if cfg.OutputPath == "" {
		cfg.OutputPath = "stdout"
	}
	if cfg.Service == "" {
		cfg.Service = "datapipe"
	}

	return newStructuredLogger(cfg.Service, cfg.Format, cfg.OutputPath, level), nil
}

type RotationLogger struct {
	logger    *structuredLogger
	maxSize   int64
	maxBackups int
	maxAge    int
	compress  bool
	mu        sync.RWMutex
}

func NewRotationLogger(service string, maxSize int64, maxBackups, maxAge int, compress bool) (*RotationLogger, error) {
	if maxSize == 0 {
		maxSize = 100 * 1024 * 1024
	}
	if maxBackups == 0 {
		maxBackups = 7
	}
	if maxAge == 0 {
		maxAge = 30
	}

	rl := &RotationLogger{
		logger:    newStructuredLogger(service, "json", "stdout", INFO),
		maxSize:   maxSize,
		maxBackups: maxBackups,
		maxAge:    maxAge,
		compress:  compress,
	}

	return rl, nil
}

func (rl *RotationLogger) Close() error {
	return rl.logger.Close()
}

func (rl *RotationLogger) SetLevel(level LogLevel) {
	rl.logger.SetLevel(level)
}

func (rl *RotationLogger) Debug(msg string, fields ...map[string]interface{}) {
	rl.logger.Debug(msg, fields...)
}

func (rl *RotationLogger) Info(msg string, fields ...map[string]interface{}) {
	rl.logger.Info(msg, fields...)
}

func (rl *RotationLogger) Warn(msg string, fields ...map[string]interface{}) {
	rl.logger.Warn(msg, fields...)
}

func (rl *RotationLogger) Error(msg string, fields ...map[string]interface{}) {
	rl.logger.Error(msg, fields...)
}

func (rl *RotationLogger) Fatal(msg string, fields ...map[string]interface{}) {
	rl.logger.Fatal(msg, fields...)
}

func (rl *RotationLogger) WithFields(fields map[string]interface{}) StructuredLogger {
	return rl.logger.WithFields(fields)
}

func (rl *RotationLogger) WithTaskID(taskID string) StructuredLogger {
	return rl.logger.WithTaskID(taskID)
}

func (rl *RotationLogger) WithPipelineID(pipelineID string) StructuredLogger {
	return rl.logger.WithPipelineID(pipelineID)
}

func (rl *RotationLogger) WithNodeID(nodeID string) StructuredLogger {
	return rl.logger.WithNodeID(nodeID)
}

func (rl *RotationLogger) WithService(service string) StructuredLogger {
	return rl.logger.WithService(service)
}

var _ StructuredLogger = (*structuredLogger)(nil)
