package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/worker/function"
	"datapipe/internal/worker/runtime"
)

type Executor struct {
	mu          sync.RWMutex
	runtime     *runtime.Runtime
	logger      logging.Logger
	maxRetries  int
	timeout     time.Duration
	running     int64
	completed   int64
	failed      int64
	taskQueue   chan *TaskRequest
	workerCount int
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

type TaskRequest struct {
	Task       *models.Task
	ExecCtx    *function.ExecutionContext
	ResultChan chan *TaskResult
}

type TaskResult struct {
	TaskID     string
	OutputData map[string]interface{}
	Error      error
	Duration   time.Duration
	RetryCount int
}

func NewExecutor(rt *runtime.Runtime, workerCount int) *Executor {
	return &Executor{
		runtime:     rt,
		logger:      logging.GetLogger().WithService("executor"),
		maxRetries:  3,
		timeout:     5 * time.Minute,
		taskQueue:   make(chan *TaskRequest, 1000),
		workerCount: workerCount,
	}
}

func (e *Executor) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	e.logger.Info("starting executor", logging.Fields{
		"worker_count": e.workerCount,
	})

	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}

	return nil
}

func (e *Executor) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	e.logger.Info("executor stopped", logging.Fields{
		"completed": atomic.LoadInt64(&e.completed),
		"failed":    atomic.LoadInt64(&e.failed),
	})
}

func (e *Executor) worker(ctx context.Context, id int) {
	defer e.wg.Done()

	e.logger.Debug("worker started", logging.Fields{"worker_id": id})

	for {
		select {
		case <-ctx.Done():
			e.logger.Debug("worker stopping", logging.Fields{"worker_id": id})
			return
		case req, ok := <-e.taskQueue:
			if !ok {
				return
			}
			e.processTask(ctx, req)
		}
	}
}

func (e *Executor) processTask(ctx context.Context, req *TaskRequest) {
	atomic.AddInt64(&e.running, 1)
	defer atomic.AddInt64(&e.running, -1)

	startTime := time.Now()
	taskLogger := e.logger.WithTaskID(req.Task.ID).WithPipelineID(req.ExecCtx.PipelineID).WithNodeID(req.ExecCtx.NodeID)

	taskLogger.Info("processing task", logging.Fields{
		"function":   req.Task.FunctionName,
		"retry_count": req.Task.RetryCount,
	})

	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			taskLogger.Info("retrying task", logging.Fields{
				"attempt": attempt,
				"max_retries": e.maxRetries,
			})
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		output, err := e.executeFunction(ctx, req.Task, req.ExecCtx)
		if err == nil {
			atomic.AddInt64(&e.completed, 1)
			taskLogger.Info("task completed", logging.Fields{
				"duration": time.Since(startTime),
			})
			req.ResultChan <- &TaskResult{
				TaskID:     req.Task.ID,
				OutputData: output,
				Error:      nil,
				Duration:   time.Since(startTime),
				RetryCount: attempt,
			}
			return
		}

		lastErr = err
		taskLogger.Warn("task execution failed", logging.Fields{
			"attempt": attempt,
			"error":   err.Error(),
		})
	}

	atomic.AddInt64(&e.failed, 1)
	taskLogger.Error("task failed after all retries", logging.Fields{
		"error":   lastErr.Error(),
		"retries": e.maxRetries,
	})
	req.ResultChan <- &TaskResult{
		TaskID:     req.Task.ID,
		OutputData: nil,
		Error:      lastErr,
		Duration:   time.Since(startTime),
		RetryCount: e.maxRetries,
	}
}

func (e *Executor) executeFunction(ctx context.Context, task *models.Task, execCtx *function.ExecutionContext) (map[string]interface{}, error) {
	fn, err := e.runtime.GetFunction(task.FunctionName, "v1")
	if err != nil {
		fn, err = e.runtime.GetFunction(task.FunctionName, "latest")
		if err != nil {
			return nil, fmt.Errorf("function not found: %s, error: %w", task.FunctionName, err)
		}
	}

	var input map[string]interface{}
	if task.InputData != nil {
		input = task.InputData
	} else {
		input = make(map[string]interface{})
	}

	output, err := e.runtime.ExecuteWithTimeout(ctx, fn, execCtx, input, e.timeout)
	if err != nil {
		return nil, fmt.Errorf("function execution failed: %w", err)
	}

	return output, nil
}

func (e *Executor) SubmitTask(task *models.Task, execCtx *function.ExecutionContext) (<-chan *TaskResult, error) {
	resultChan := make(chan *TaskResult, 1)
	req := &TaskRequest{
		Task:       task,
		ExecCtx:    execCtx,
		ResultChan: resultChan,
	}
	select {
	case e.taskQueue <- req:
		return resultChan, nil
	default:
		return nil, fmt.Errorf("task queue is full")
	}
}

func (e *Executor) ExecuteTaskSync(ctx context.Context, task *models.Task, execCtx *function.ExecutionContext) (*TaskResult, error) {
	resultCh, err := e.SubmitTask(task, execCtx)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result, nil
	}
}

func (e *Executor) GetStats() ExecutorStats {
	return ExecutorStats{
		Running:   atomic.LoadInt64(&e.running),
		Completed: atomic.LoadInt64(&e.completed),
		Failed:    atomic.LoadInt64(&e.failed),
		QueueSize: len(e.taskQueue),
	}
}

type ExecutorStats struct {
	Running   int64
	Completed int64
	Failed    int64
	QueueSize int
}

func (e *Executor) SetLogger(logger logging.Logger) {
	e.logger = logger
}

func (e *Executor) SetMaxRetries(max int) {
	e.maxRetries = max
}

func (e *Executor) SetTimeout(timeout time.Duration) {
	e.timeout = timeout
}

func (e *Executor) ExecuteFunctionChain(ctx context.Context, tasks []*models.Task, execCtx *function.ExecutionContext) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0, len(tasks))

	for i, task := range tasks {
		result, err := e.ExecuteTaskSync(ctx, task, execCtx)
		if err != nil {
			return results, fmt.Errorf("task %d (%s) failed: %w", i, task.FunctionName, err)
		}

		if result.Error != nil {
			return results, fmt.Errorf("task %d (%s) error: %w", i, task.FunctionName, result.Error)
		}

		results = append(results, result.OutputData)

		if i < len(tasks)-1 && result.OutputData != nil {
			nextTask := tasks[i+1]
			if nextTask.InputData == nil {
				nextTask.InputData = make(map[string]interface{})
			}
			for k, v := range result.OutputData {
				nextTask.InputData[k] = v
			}
		}
	}

	return results, nil
}
