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

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func ParseLevel(s string) Level {
	switch s {
	case "DEBUG", "debug":
		return DEBUG
	case "INFO", "info":
		return INFO
	case "WARN", "warn":
		return WARN
	case "ERROR", "error":
		return ERROR
	default:
		return INFO
	}
}

type Fields map[string]interface{}

type LogEntry struct {
	Timestamp  string                 `json:"timestamp"`
	Level      string                 `json:"level"`
	Service    string                 `json:"service,omitempty"`
	TaskID     string                 `json:"task_id,omitempty"`
	PipelineID string                 `json:"pipeline_id,omitempty"`
	NodeID     string                 `json:"node_id,omitempty"`
	Message    string                 `json:"message"`
	Caller     string                 `json:"caller,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

type Logger interface {
	Debug(msg string, fields ...Fields)
	Info(msg string, fields ...Fields)
	Warn(msg string, fields ...Fields)
	Error(msg string, fields ...Fields)
	WithFields(fields Fields) Logger
	WithTaskID(taskID string) Logger
	WithPipelineID(pipelineID string) Logger
	WithNodeID(nodeID string) Logger
	WithService(service string) Logger
	WithWriter(w io.Writer) Logger
	SetLevel(level Level)
	GetLevel() Level
	Close() error
}

type logger struct {
	mu         sync.RWMutex
	level      Level
	format     string
	outputPath string
	service    string
	taskID     string
	pipelineID string
	nodeID     string
	writer     io.Writer
	fields     Fields
	closeFunc  func() error
}

var (
	defaultLogger *logger
	loggerOnce    sync.Once
)

func New(service string) Logger {
	return newLogger(service, "json", "stdout", INFO)
}

func newLogger(service, format, outputPath string, level Level) *logger {
	l := &logger{
		level:      level,
		format:     format,
		outputPath: outputPath,
		service:    service,
		writer:     os.Stdout,
		fields:     make(Fields),
	}
	return l
}

func GetLogger() Logger {
	if defaultLogger == nil {
		loggerOnce.Do(func() {
			defaultLogger = newLogger("datapipe", "json", "stdout", INFO)
		})
	}
	return defaultLogger
}

func SetLogger(l Logger) {
	if l != nil {
		if defLogger, ok := l.(*logger); ok {
			defaultLogger = defLogger
		}
	}
}

func Debug(msg string, fields ...Fields) {
	GetLogger().Debug(msg, fields...)
}

func Info(msg string, fields ...Fields) {
	GetLogger().Info(msg, fields...)
}

func Warn(msg string, fields ...Fields) {
	GetLogger().Warn(msg, fields...)
}

func Error(msg string, fields ...Fields) {
	GetLogger().Error(msg, fields...)
}

func (l *logger) output(level Level, msg string, fields ...Fields) {
	if level < l.level {
		return
	}

	l.mu.RLock()
	entry := LogEntry{
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
		entry.Fields = fields[0]
	} else if len(l.fields) > 0 {
		entry.Fields = l.fields
	}
	l.mu.RUnlock()

	l.writeEntry(&entry)
}

func (l *logger) getCaller() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

func (l *logger) writeEntry(entry *LogEntry) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var data []byte
	var err error

	if l.format == "json" {
		data, err = json.Marshal(entry)
	} else {
		data, err = l.formatText(entry)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal log entry: %v\n", err)
		return
	}

	l.writer.Write(data)
	l.writer.Write([]byte("\n"))
}

func (l *logger) formatText(entry *LogEntry) ([]byte, error) {
	format := "[%s] %s | %s | %s"
	args := []interface{}{
		entry.Timestamp,
		entry.Level,
		entry.Service,
		entry.Message,
	}

	if entry.TaskID != "" {
		format += " | task_id=%s"
		args = append(args, entry.TaskID)
	}
	if entry.PipelineID != "" {
		format += " | pipeline_id=%s"
		args = append(args, entry.PipelineID)
	}
	if entry.NodeID != "" {
		format += " | node_id=%s"
		args = append(args, entry.NodeID)
	}
	if entry.Caller != "" {
		format += " | %s"
		args = append(args, entry.Caller)
	}
	if len(entry.Fields) > 0 {
		format += " | fields=%v"
		args = append(args, entry.Fields)
	}

	return []byte(fmt.Sprintf(format, args...)), nil
}

func (l *logger) Debug(msg string, fields ...Fields) {
	l.output(DEBUG, msg, fields...)
}

func (l *logger) Info(msg string, fields ...Fields) {
	l.output(INFO, msg, fields...)
}

func (l *logger) Warn(msg string, fields ...Fields) {
	l.output(WARN, msg, fields...)
}

func (l *logger) Error(msg string, fields ...Fields) {
	l.output(ERROR, msg, fields...)
}

func (l *logger) WithFields(fields Fields) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := &logger{
		level:      l.level,
		format:     l.format,
		outputPath: l.outputPath,
		service:    l.service,
		taskID:     l.taskID,
		pipelineID: l.pipelineID,
		nodeID:     l.nodeID,
		writer:     l.writer,
		fields:     make(Fields),
	}

	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	for k, v := range fields {
		newLogger.fields[k] = v
	}

	return newLogger
}

func (l *logger) WithTaskID(taskID string) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.taskID = taskID
	newLogger.fields = make(Fields)
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

func (l *logger) WithPipelineID(pipelineID string) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.pipelineID = pipelineID
	newLogger.fields = make(Fields)
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

func (l *logger) WithNodeID(nodeID string) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.nodeID = nodeID
	newLogger.fields = make(Fields)
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

func (l *logger) WithService(service string) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.service = service
	newLogger.fields = make(Fields)
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

func (l *logger) WithWriter(w io.Writer) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newLogger := *l
	newLogger.writer = w
	newLogger.fields = make(Fields)
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

func (l *logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *logger) GetLevel() Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

func (l *logger) Close() error {
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

type LoggerConfig struct {
	Level      string
	Format     string
	OutputPath string
	Service    string
}

func NewLoggerWithConfig(cfg LoggerConfig) (Logger, error) {
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

	logger := &logger{
		level:      level,
		format:     cfg.Format,
		outputPath: cfg.OutputPath,
		service:    cfg.Service,
		writer:     os.Stdout,
		fields:     make(Fields),
	}

	if cfg.OutputPath != "stdout" && cfg.OutputPath != "stderr" {
		file, err := os.OpenFile(cfg.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		logger.writer = file
		logger.closeFunc = file.Close
	}

	return logger, nil
}

type BufferedLogger struct {
	logger  *logger
	buffer  chan *LogEntry
	wg      sync.WaitGroup
	done    chan struct{}
	bufSize int
}

func NewBufferedLogger(service string, bufSize int) *BufferedLogger {
	bl := &BufferedLogger{
		logger:  newLogger(service, "json", "stdout", INFO),
		buffer:  make(chan *LogEntry, bufSize),
		done:    make(chan struct{}),
		bufSize: bufSize,
	}

	bl.wg.Add(1)
	go bl.processLogs()

	return bl
}

func (bl *BufferedLogger) processLogs() {
	defer bl.wg.Done()
	for {
		select {
		case entry := <-bl.buffer:
			bl.logger.writeEntry(entry)
		case <-bl.done:
			for {
				select {
				case entry := <-bl.buffer:
					bl.logger.writeEntry(entry)
				default:
					return
				}
			}
		}
	}
}

func (bl *BufferedLogger) Debug(msg string, fields ...Fields) {
	bl.log(DEBUG, msg, fields...)
}

func (bl *BufferedLogger) Info(msg string, fields ...Fields) {
	bl.log(INFO, msg, fields...)
}

func (bl *BufferedLogger) Warn(msg string, fields ...Fields) {
	bl.log(WARN, msg, fields...)
}

func (bl *BufferedLogger) Error(msg string, fields ...Fields) {
	bl.log(ERROR, msg, fields...)
}

func (bl *BufferedLogger) log(level Level, msg string, fields ...Fields) {
	if level < bl.logger.level {
		return
	}

	entry := &LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Level:      level.String(),
		Service:    bl.logger.service,
		TaskID:     bl.logger.taskID,
		PipelineID: bl.logger.pipelineID,
		NodeID:     bl.logger.nodeID,
		Message:    msg,
		Caller:     bl.logger.getCaller(),
	}

	if len(fields) > 0 && fields[0] != nil {
		entry.Fields = fields[0]
	}

	select {
	case bl.buffer <- entry:
	default:
	}
}

func (bl *BufferedLogger) Close() error {
	close(bl.done)
	bl.wg.Wait()
	return bl.logger.Close()
}

func (bl *BufferedLogger) SetLevel(level Level) {
	bl.logger.SetLevel(level)
}

func (bl *BufferedLogger) GetLevel() Level {
	return bl.logger.GetLevel()
}

func (bl *BufferedLogger) WithFields(fields Fields) Logger {
	return bl.logger.WithFields(fields)
}

func (bl *BufferedLogger) WithTaskID(taskID string) Logger {
	return bl.logger.WithTaskID(taskID)
}

func (bl *BufferedLogger) WithPipelineID(pipelineID string) Logger {
	return bl.logger.WithPipelineID(pipelineID)
}

func (bl *BufferedLogger) WithNodeID(nodeID string) Logger {
	return bl.logger.WithNodeID(nodeID)
}

func (bl *BufferedLogger) WithService(service string) Logger {
	return bl.logger.WithService(service)
}

func (bl *BufferedLogger) WithWriter(w io.Writer) Logger {
	return bl.logger.WithWriter(w)
}

var _ Logger = (*logger)(nil)
var _ Logger = (*BufferedLogger)(nil)
