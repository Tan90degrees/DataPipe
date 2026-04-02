package logging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LogStore struct {
	mu      sync.RWMutex
	logs    []*LogEntry
	index   map[string][]int
	maxSize int
}

func NewLogStore(maxSize int) *LogStore {
	if maxSize == 0 {
		maxSize = 100000
	}
	return &LogStore{
		logs:    make([]*LogEntry, 0, maxSize),
		index:   make(map[string][]int),
		maxSize: maxSize,
	}
}

func (s *LogStore) Add(entry *LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.logs) >= s.maxSize {
		s.logs = s.logs[1:]
	}

	idx := len(s.logs)
	s.logs = append(s.logs, entry)

	if entry.TaskID != "" {
		s.index["task_id:"+entry.TaskID] = append(s.index["task_id:"+entry.TaskID], idx)
	}
	if entry.PipelineID != "" {
		s.index["pipeline_id:"+entry.PipelineID] = append(s.index["pipeline_id:"+entry.PipelineID], idx)
	}
	if entry.NodeID != "" {
		s.index["node_id:"+entry.NodeID] = append(s.index["node_id:"+entry.NodeID], idx)
	}
	if entry.Service != "" {
		s.index["service:"+entry.Service] = append(s.index["service:"+entry.Service], idx)
	}
	s.index["level:"+entry.Level] = append(s.index["level:"+entry.Level], idx)
}

func (s *LogStore) Query(req LogQueryRequest) []*LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*LogEntry

	for _, entry := range s.logs {
		if req.TaskID != "" && entry.TaskID != req.TaskID {
			continue
		}
		if req.PipelineID != "" && entry.PipelineID != req.PipelineID {
			continue
		}
		if req.NodeID != "" && entry.NodeID != req.NodeID {
			continue
		}
		if req.Service != "" && entry.Service != req.Service {
			continue
		}
		if req.Level != "" && entry.Level != req.Level {
			continue
		}
		if !req.StartTime.IsZero() {
			if entry.Timestamp < req.StartTime.Format(time.RFC3339Nano) {
				continue
			}
		}
		if !req.EndTime.IsZero() {
			if entry.Timestamp > req.EndTime.Format(time.RFC3339Nano) {
				continue
			}
		}
		if req.MessageContains != "" {
			if !contains(entry.Message, req.MessageContains) {
				continue
			}
		}

		result = append(result, entry)
	}

	if req.Limit > 0 && len(result) > req.Limit {
		result = result[len(result)-req.Limit:]
	}

	return result
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (s *LogStore) GetAll() []*LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*LogEntry, len(s.logs))
	copy(result, s.logs)
	return result
}

func (s *LogStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.logs)
}

func (s *LogStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = make([]*LogEntry, 0, s.maxSize)
	s.index = make(map[string][]int)
}

type LogQueryRequest struct {
	TaskID           string
	PipelineID       string
	NodeID           string
	Service          string
	Level            string
	StartTime        time.Time
	EndTime          time.Time
	MessageContains  string
	Limit            int
	Offset           int
}

type LogExportRequest struct {
	Format    string
	StartTime time.Time
	EndTime   time.Time
	TaskID    string
	PipelineID string
	NodeID    string
	Service   string
	Level     string
}

type LogAggregator struct {
	mu      sync.RWMutex
	store   *LogStore
	logChan chan *LogEntry
	stopCh  chan struct{}
	wg      sync.WaitGroup
	httpServer *http.Server
}

func NewLogAggregator(maxStoreSize int) *LogAggregator {
	return &LogAggregator{
		store:   NewLogStore(maxStoreSize),
		logChan: make(chan *LogEntry, 10000),
		stopCh:  make(chan struct{}),
	}
}

func (a *LogAggregator) Start(addr string) error {
	a.wg.Add(2)
	go a.processLogs()
	go a.serveHTTP(addr)
	return nil
}

func (a *LogAggregator) Stop() error {
	close(a.stopCh)
	a.wg.Wait()

	if a.httpServer != nil {
		return a.httpServer.Close()
	}
	return nil
}

func (a *LogAggregator) processLogs() {
	defer a.wg.Done()

	for {
		select {
		case entry := <-a.logChan:
			a.store.Add(entry)
		case <-a.stopCh:
			for {
				select {
				case entry := <-a.logChan:
					a.store.Add(entry)
				default:
					return
				}
			}
		}
	}
}

func (a *LogAggregator) serveHTTP(addr string) {
	defer a.wg.Done()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/logs", a.handleCollectLogs)
	mux.HandleFunc("/api/logs/query", a.handleQueryLogs)
	mux.HandleFunc("/api/logs/export", a.handleExportLogs)
	mux.HandleFunc("/api/logs/stats", a.handleStats)
	mux.HandleFunc("/health", a.handleHealth)

	a.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	a.httpServer.ListenAndServe()
}

func (a *LogAggregator) handleCollectLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if entry.Timestamp == "" {
		entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}

	select {
	case a.logChan <- &entry:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"ok"}`))
	default:
		http.Error(w, "Log buffer full", http.StatusServiceUnavailable)
	}
}

func (a *LogAggregator) handleQueryLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LogQueryRequest

	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	} else {
		query := r.URL.Query()
		req.TaskID = query.Get("task_id")
		req.PipelineID = query.Get("pipeline_id")
		req.NodeID = query.Get("node_id")
		req.Service = query.Get("service")
		req.Level = query.Get("level")
		req.MessageContains = query.Get("message")
	}

	results := a.store.Query(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total": len(results),
		"logs":  results,
	})
}

func (a *LogAggregator) handleExportLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LogExportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	queryReq := LogQueryRequest{
		TaskID:     req.TaskID,
		PipelineID: req.PipelineID,
		NodeID:     req.NodeID,
		Service:    req.Service,
		Level:      req.Level,
		StartTime:  req.StartTime,
		EndTime:    req.EndTime,
	}

	results := a.store.Query(queryReq)

	if req.Format == "json" || req.Format == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	} else if req.Format == "text" {
		w.Header().Set("Content-Type", "text/plain")
		for _, entry := range results {
			fmt.Fprintf(w, "[%s] %s | %s | %s | %s\n",
				entry.Timestamp, entry.Level, entry.Service, entry.Message, formatContext(entry.Context))
		}
	} else if req.Format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Write([]byte("timestamp,level,service,task_id,pipeline_id,node_id,message\n"))
		for _, entry := range results {
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s\n",
				entry.Timestamp, entry.Level, entry.Service, entry.TaskID, entry.PipelineID, entry.NodeID, entry.Message)
		}
	}
}

func formatContext(ctx map[string]interface{}) string {
	if ctx == nil {
		return ""
	}
	data, _ := json.Marshal(ctx)
	return string(data)
}

func (a *LogAggregator) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_logs": a.store.Size(),
	})
}

func (a *LogAggregator) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (a *LogAggregator) Collect(entry *LogEntry) {
	select {
	case a.logChan <- entry:
	default:
	}
}

type FileLogCollector struct {
	mu        sync.RWMutex
	logFiles  []string
	outputDir string
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func NewFileLogCollector(outputDir string) *FileLogCollector {
	if outputDir == "" {
		outputDir = "./logs"
	}
	return &FileLogCollector{
		outputDir: outputDir,
		logFiles:  make([]string, 0),
		stopCh:   make(chan struct{}),
	}
}

func (c *FileLogCollector) Start() error {
	if err := os.MkdirAll(c.outputDir, 0755); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.scanLogs()
	return nil
}

func (c *FileLogCollector) Stop() error {
	close(c.stopCh)
	c.wg.Wait()
	return nil
}

func (c *FileLogCollector) scanLogs() {
	defer c.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.collectLogFiles()
		case <-c.stopCh:
			return
		}
	}
}

func (c *FileLogCollector) collectLogFiles() {
	c.mu.Lock()
	defer c.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(c.outputDir, "*.log"))
	if err != nil {
		return
	}

	c.logFiles = files
}

func (c *FileLogCollector) GetLogFiles() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, len(c.logFiles))
	copy(result, c.logFiles)
	return result
}

type LogAggregationConfig struct {
	HTTPAddr     string
	MaxStoreSize int
	BufferSize   int
}

func NewLogAggregationConfig() *LogAggregationConfig {
	return &LogAggregationConfig{
		HTTPAddr:     ":8081",
		MaxStoreSize: 100000,
		BufferSize:   10000,
	}
}

var globalAggregator *LogAggregator
var aggregatorOnce sync.Once

func GetLogAggregator() *LogAggregator {
	if globalAggregator == nil {
		aggregatorOnce.Do(func() {
			globalAggregator = NewLogAggregator(100000)
		})
	}
	return globalAggregator
}

func StartLogAggregator(addr string) error {
	agg := GetLogAggregator()
	return agg.Start(addr)
}

func StopLogAggregator() error {
	agg := GetLogAggregator()
	return agg.Stop()
}
