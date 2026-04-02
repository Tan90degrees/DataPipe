package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/worker/config"
	"datapipe/internal/worker/executor"
	"datapipe/internal/worker/function"
	"datapipe/internal/worker/runtime"

	"github.com/google/uuid"
)

type Worker struct {
	mu           sync.RWMutex
	id           string
	name         string
	config       *config.Config
	logger       logging.Logger
	runtime      *runtime.Runtime
	executor     *executor.Executor
	registry     *function.Registry
	status       WorkerStatus
	httpServer   *http.Server
	masterClient *MasterClient
	heartbeatTicker *time.Ticker
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	runningTasks   int64
	completedTasks int64
	failedTasks    int64
	totalTasks     int64
}

type WorkerStatus string

const (
	WorkerStatusInit     WorkerStatus = "init"
	WorkerStatusStarting WorkerStatus = "starting"
	WorkerStatusRunning  WorkerStatus = "running"
	WorkerStatusStopping WorkerStatus = "stopping"
	WorkerStatusStopped  WorkerStatus = "stopped"
	WorkerStatusFailed   WorkerStatus = "failed"
)

type MasterClient struct {
	host       string
	port       int
	httpClient *http.Client
	timeout    time.Duration
}

func NewMasterClient(host string, port int) *MasterClient {
	return &MasterClient{
		host:    host,
		port:    port,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		timeout: 30 * time.Second,
	}
}

func (c *MasterClient) GetBaseURL() string {
	return fmt.Sprintf("http://%s:%d", c.host, c.port)
}

func (c *MasterClient) Register(ctx context.Context, worker *Worker) (*RegisterResponse, error) {
	url := c.GetBaseURL() + "/api/v1/workers/register"

	req := RegisterRequest{
		WorkerID:   worker.id,
		WorkerName: worker.name,
		Host:       worker.config.Worker.Host,
		Port:       worker.config.Worker.Port,
		Status:     string(WorkerStatusRunning),
		Functions:  worker.registry.ListMetadata(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (c *MasterClient) SendHeartbeat(ctx context.Context, worker *Worker) error {
	url := c.GetBaseURL() + "/api/v1/workers/heartbeat"

	stats := executor.ExecutorStats{}
	if worker.executor != nil {
		stats = worker.executor.GetStats()
	}

	req := HeartbeatRequest{
		WorkerID:       worker.id,
		Status:         string(worker.status),
		RunningTasks:   atomic.LoadInt64(&worker.runningTasks),
		CompletedTasks: atomic.LoadInt64(&worker.completedTasks),
		FailedTasks:    atomic.LoadInt64(&worker.failedTasks),
		ExecutorStats:  stats,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *MasterClient) ReportTaskResult(ctx context.Context, workerID string, result *TaskResult) error {
	url := c.GetBaseURL() + "/api/v1/workers/tasks/result"

	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

type RegisterRequest struct {
	WorkerID   string                   `json:"worker_id"`
	WorkerName string                   `json:"worker_name"`
	Host       string                   `json:"host"`
	Port       int                      `json:"port"`
	Status     string                   `json:"status"`
	Functions  []function.FunctionMetadata `json:"functions"`
}

type RegisterResponse struct {
	WorkerID  string   `json:"worker_id"`
	MasterID  string   `json:"master_id"`
	SessionID string   `json:"session_id"`
	Functions []string `json:"functions"`
}

type HeartbeatRequest struct {
	WorkerID       string             `json:"worker_id"`
	Status         string             `json:"status"`
	RunningTasks   int64              `json:"running_tasks"`
	CompletedTasks int64              `json:"completed_tasks"`
	FailedTasks    int64              `json:"failed_tasks"`
	ExecutorStats  executor.ExecutorStats `json:"executor_stats"`
}

type TaskResult struct {
	WorkerID     string                 `json:"worker_id"`
	TaskID       string                 `json:"task_id"`
	ExecutionID  string                 `json:"execution_id"`
	Status       models.TaskStatus      `json:"status"`
	OutputData   map[string]interface{} `json:"output_data,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Duration     time.Duration         `json:"duration"`
	Timestamp    time.Time              `json:"timestamp"`
}

func New(cfg *config.Config) (*Worker, error) {
	if cfg.Worker.ID == "" {
		cfg.Worker.ID = uuid.New().String()
	}
	if cfg.Worker.Name == "" {
		cfg.Worker.Name = fmt.Sprintf("worker-%s", cfg.Worker.ID[:8])
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		id:         cfg.Worker.ID,
		name:       cfg.Worker.Name,
		config:     cfg,
		status:     WorkerStatusInit,
		ctx:       ctx,
		cancel:    cancel,
	}

	w.logger = logging.GetLogger().WithService("worker").WithFields(logging.Fields{
		"worker_id": w.id,
		"worker_name": w.name,
	})

	w.registry = function.NewRegistry()
	w.runtime = runtime.NewRuntime(w.registry)
	w.executor = executor.NewExecutor(w.runtime, cfg.Executor.WorkerCount)

	if cfg.Master.Host != "" && cfg.Master.Port != 0 {
		w.masterClient = NewMasterClient(cfg.Master.Host, cfg.Master.Port)
	}

	return w, nil
}

func (w *Worker) Start() error {
	w.mu.Lock()
	if w.status == WorkerStatusRunning {
		w.mu.Unlock()
		return fmt.Errorf("worker already running")
	}
	w.status = WorkerStatusStarting
	w.mu.Unlock()

	w.logger.Info("starting worker", logging.Fields{
		"worker_id":   w.id,
		"worker_name": w.name,
		"host":        w.config.Worker.Host,
		"port":        w.config.Worker.Port,
	})

	if err := w.registerToMaster(); err != nil {
		w.logger.Warn("failed to register to master, continuing anyway", logging.Fields{
			"error": err.Error(),
		})
	}

	if err := w.executor.Start(w.ctx); err != nil {
		return fmt.Errorf("failed to start executor: %w", err)
	}

	w.startHeartbeat()

	if err := w.startHTTPServer(); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	w.mu.Lock()
	w.status = WorkerStatusRunning
	w.mu.Unlock()

	w.logger.Info("worker started successfully", logging.Fields{
		"worker_id": w.id,
		"addr":      w.config.Worker.GetAddr(),
	})

	return nil
}

func (w *Worker) Stop() error {
	w.mu.Lock()
	if w.status != WorkerStatusRunning {
		w.mu.Unlock()
		return fmt.Errorf("worker is not running")
	}
	w.status = WorkerStatusStopping
	w.mu.Unlock()

	w.logger.Info("stopping worker", logging.Fields{"worker_id": w.id})

	if w.heartbeatTicker != nil {
		w.heartbeatTicker.Stop()
	}

	w.executor.Stop()

	if w.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), w.config.Worker.ShutdownTimeout)
		defer cancel()
		if err := w.httpServer.Shutdown(shutdownCtx); err != nil {
			w.logger.Error("failed to shutdown HTTP server", logging.Fields{"error": err.Error()})
		}
	}

	w.deregisterFromMaster()

	w.mu.Lock()
	w.status = WorkerStatusStopped
	w.mu.Unlock()

	w.logger.Info("worker stopped", logging.Fields{
		"completed_tasks": atomic.LoadInt64(&w.completedTasks),
		"failed_tasks":    atomic.LoadInt64(&w.failedTasks),
	})

	return nil
}

func (w *Worker) registerToMaster() error {
	if w.masterClient == nil {
		return fmt.Errorf("master client not configured")
	}

	ctx, cancel := context.WithTimeout(w.ctx, w.config.Worker.RegisterTimeout)
	defer cancel()

	resp, err := w.masterClient.Register(ctx, w)
	if err != nil {
		return fmt.Errorf("failed to register to master: %w", err)
	}

	w.logger.Info("registered to master", logging.Fields{
		"master_id":  resp.MasterID,
		"session_id": resp.SessionID,
	})

	return nil
}

func (w *Worker) deregisterFromMaster() {
	if w.masterClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := HeartbeatRequest{
		WorkerID: w.id,
		Status:   string(WorkerStatusStopped),
	}

	body, _ := json.Marshal(req)
	url := w.masterClient.GetBaseURL() + "/api/v1/workers/deregister"
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.masterClient.httpClient.Do(httpReq)
	if err != nil {
		w.logger.Error("failed to deregister from master", logging.Fields{"error": err.Error()})
	} else {
		resp.Body.Close()
	}
}

func (w *Worker) startHeartbeat() {
	w.heartbeatTicker = time.NewTicker(w.config.Worker.HeartbeatInterval)

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for {
			select {
			case <-w.ctx.Done():
				return
			case <-w.heartbeatTicker.C:
				w.sendHeartbeat()
			}
		}
	}()
}

func (w *Worker) sendHeartbeat() {
	if w.masterClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(w.ctx, 10*time.Second)
	defer cancel()

	if err := w.masterClient.SendHeartbeat(ctx, w); err != nil {
		w.logger.Warn("failed to send heartbeat", logging.Fields{
			"error": err.Error(),
		})
	}
}

func (w *Worker) startHTTPServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", w.handleHealth)
	mux.HandleFunc("/status", w.handleStatus)
	mux.HandleFunc("/metrics", w.handleMetrics)

	addr := w.config.Worker.GetAddr()
	w.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := w.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			w.logger.Error("HTTP server error", logging.Fields{"error": err.Error()})
		}
	}()

	return nil
}

func (w *Worker) handleHealth(wr http.ResponseWriter, r *http.Request) {
	w.mu.RLock()
	status := w.status
	w.mu.RUnlock()

	if status == WorkerStatusRunning {
		wr.WriteHeader(http.StatusOK)
		json.NewEncoder(wr).Encode(map[string]string{"status": "healthy"})
	} else {
		wr.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(wr).Encode(map[string]string{"status": "unhealthy"})
	}
}

func (w *Worker) handleStatus(wr http.ResponseWriter, r *http.Request) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	status := WorkerInfo{
		ID:             w.id,
		Name:           w.name,
		Status:         string(w.status),
		Host:           w.config.Worker.Host,
		Port:           w.config.Worker.Port,
		RunningTasks:   atomic.LoadInt64(&w.runningTasks),
		CompletedTasks: atomic.LoadInt64(&w.completedTasks),
		FailedTasks:    atomic.LoadInt64(&w.failedTasks),
		TotalTasks:     atomic.LoadInt64(&w.totalTasks),
		Functions:       w.registry.ListMetadata(),
	}

	wr.Header().Set("Content-Type", "application/json")
	json.NewEncoder(wr).Encode(status)
}

func (w *Worker) handleMetrics(wr http.ResponseWriter, r *http.Request) {
	stats := w.executor.GetStats()

	metrics := map[string]interface{}{
		"worker_id":        w.id,
		"running_tasks":    atomic.LoadInt64(&w.runningTasks),
		"completed_tasks":  atomic.LoadInt64(&w.completedTasks),
		"failed_tasks":     atomic.LoadInt64(&w.failedTasks),
		"total_tasks":      atomic.LoadInt64(&w.totalTasks),
		"executor":         stats,
	}

	wr.Header().Set("Content-Type", "application/json")
	json.NewEncoder(wr).Encode(metrics)
}

type WorkerInfo struct {
	ID             string                      `json:"id"`
	Name           string                      `json:"name"`
	Status         string                      `json:"status"`
	Host           string                      `json:"host"`
	Port           int                         `json:"port"`
	RunningTasks   int64                       `json:"running_tasks"`
	CompletedTasks int64                       `json:"completed_tasks"`
	FailedTasks    int64                       `json:"failed_tasks"`
	TotalTasks     int64                       `json:"total_tasks"`
	Functions      []function.FunctionMetadata  `json:"functions"`
}

func (w *Worker) ExecuteTask(task *models.Task, execCtx *function.ExecutionContext) (*TaskResult, error) {
	atomic.AddInt64(&w.runningTasks, 1)
	atomic.AddInt64(&w.totalTasks, 1)
	defer atomic.AddInt64(&w.runningTasks, -1)

	startTime := time.Now()

	result, err := w.executor.ExecuteTaskSync(w.ctx, task, execCtx)

	duration := time.Since(startTime)

	if err != nil {
		atomic.AddInt64(&w.failedTasks, 1)
		return &TaskResult{
			WorkerID:     w.id,
			TaskID:       task.ID,
			ExecutionID:  execCtx.PipelineID,
			Status:       models.TaskStatusFailed,
			ErrorMessage: err.Error(),
			Duration:     duration,
			Timestamp:    time.Now(),
		}, err
	}

	if result.Error != nil {
		atomic.AddInt64(&w.failedTasks, 1)
		return &TaskResult{
			WorkerID:     w.id,
			TaskID:       task.ID,
			ExecutionID:  execCtx.PipelineID,
			Status:       models.TaskStatusFailed,
			OutputData:   result.OutputData,
			ErrorMessage: result.Error.Error(),
			Duration:     duration,
			Timestamp:    time.Now(),
		}, result.Error
	}

	atomic.AddInt64(&w.completedTasks, 1)

	taskResult := &TaskResult{
		WorkerID:    w.id,
		TaskID:      task.ID,
		ExecutionID: execCtx.PipelineID,
		Status:      models.TaskStatusCompleted,
		OutputData:  result.OutputData,
		Duration:    duration,
		Timestamp:   time.Now(),
	}

	if w.masterClient != nil {
		reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w.masterClient.ReportTaskResult(reportCtx, w.id, taskResult)
	}

	return taskResult, nil
}

func (w *Worker) RegisterFunction(fn function.Function) error {
	return w.registry.Register(fn)
}

func (w *Worker) GetRuntime() *runtime.Runtime {
	return w.runtime
}

func (w *Worker) GetExecutor() *executor.Executor {
	return w.executor
}

func (w *Worker) GetRegistry() *function.Registry {
	return w.registry
}

func (w *Worker) GetID() string {
	return w.id
}

func (w *Worker) GetName() string {
	return w.name
}

func (w *Worker) GetStatus() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}