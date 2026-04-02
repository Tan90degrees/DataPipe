package sharding

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"datapipe/internal/common/errors"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	ID        string
	Shard     *Shard
	Status    TaskStatus
	WorkerID  string
	Result    interface{}
	Error     *errors.Error
	StartTime time.Time
	EndTime   time.Time
	Retries   int
}

func NewTask(id string, shard *Shard) *Task {
	return &Task{
		ID:        id,
		Shard:     shard,
		Status:    TaskStatusPending,
		StartTime: time.Time{},
		EndTime:   time.Time{},
		Retries:   0,
	}
}

func (t *Task) MarkRunning(workerID string) {
	t.Status = TaskStatusRunning
	t.WorkerID = workerID
	t.StartTime = time.Now()
}

func (t *Task) MarkCompleted(result interface{}) {
	t.Status = TaskStatusCompleted
	t.Result = result
	t.EndTime = time.Now()
}

func (t *Task) MarkFailed(err *errors.Error) {
	t.Status = TaskStatusFailed
	t.Error = err
	t.EndTime = time.Now()
}

func (t *Task) MarkCancelled() {
	t.Status = TaskStatusCancelled
	t.EndTime = time.Now()
}

func (t *Task) Duration() time.Duration {
	if t.EndTime.IsZero() {
		return time.Since(t.StartTime)
	}
	return t.EndTime.Sub(t.StartTime)
}

type Worker interface {
	ID() string
	Capacity() int
	IsAvailable() bool
	Assign(ctx context.Context, task *Task) error
	Heartbeat() error
}

type Scheduler interface {
	Schedule(ctx context.Context, shards []*Shard) ([]*Task, error)
	Wait(ctx context.Context) error
	Results() []*Task
	Cancel()
}

type TaskExecutor func(ctx context.Context, task *Task) (interface{}, error)

type DefaultScheduler struct {
	mu         sync.RWMutex
	workers    []Worker
	tasks      []*Task
	taskIndex  int32
	executor   TaskExecutor
	maxRetries int
	timeout    time.Duration

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	resultCh   chan *Task
	errorCh    chan *errors.Error
}

type SchedulerConfig struct {
	NumWorkers   int           `json:"num_workers"`
	MaxRetries   int           `json:"max_retries"`
	Timeout      time.Duration `json:"timeout"`
	BatchSize    int           `json:"batch_size"`
	QueueSize    int           `json:"queue_size"`
}

func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		NumWorkers:   4,
		MaxRetries:   3,
		Timeout:      5 * time.Minute,
		BatchSize:    10,
		QueueSize:    100,
	}
}

func NewDefaultScheduler(workers []Worker, executor TaskExecutor, config *SchedulerConfig) *DefaultScheduler {
	if config == nil {
		config = NewSchedulerConfig()
	}

	return &DefaultScheduler{
		workers:    workers,
		tasks:      make([]*Task, 0),
		taskIndex:  0,
		executor:   executor,
		maxRetries: config.MaxRetries,
		timeout:    config.Timeout,
		resultCh:   make(chan *Task, config.QueueSize),
		errorCh:    make(chan *errors.Error, config.QueueSize),
	}
}

func (s *DefaultScheduler) Schedule(ctx context.Context, shards []*Shard) ([]*Task, error) {
	s.mu.Lock()
	s.tasks = make([]*Task, 0, len(shards))
	for i, shard := range shards {
		task := &Task{
			ID:        generateTaskID(i),
			Shard:     shard,
			Status:    TaskStatusPending,
			Retries:   0,
		}
		s.tasks = append(s.tasks, task)
	}
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	s.wg.Add(len(shards))
	go s.dispatchLoop(ctx, shards)

	return s.tasks, nil
}

func (s *DefaultScheduler) dispatchLoop(ctx context.Context, shards []*Shard) {
	taskQueue := make(chan *Task, len(shards))
	for _, task := range s.tasks {
		taskQueue <- task
	}
	close(taskQueue)

	workerPool := make(chan Worker, len(s.workers))
	for _, w := range s.workers {
		workerPool <- w
	}

	var wg sync.WaitGroup

	for task := range taskQueue {
		select {
		case <-ctx.Done():
			break
		case worker := <-workerPool:
			wg.Add(1)
			go func(t *Task, w Worker) {
				defer wg.Done()
				s.executeTask(ctx, t, w)
				workerPool <- w
			}(task, worker)
		}
	}

	wg.Wait()
	close(s.resultCh)
	close(s.errorCh)
}

func (s *DefaultScheduler) executeTask(ctx context.Context, task *Task, worker Worker) {
	taskCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	task.MarkRunning(worker.ID())

	result, err := s.executor(taskCtx, task)
	if err != nil {
		task.MarkFailed(errors.Wrap(err, errors.ErrCodeTaskFailed, "task execution failed"))
		s.errorCh <- task.Error
	} else {
		task.MarkCompleted(result)
		s.resultCh <- task
	}

	s.wg.Done()
}

func (s *DefaultScheduler) Wait(ctx context.Context) error {
	s.wg.Wait()
	return nil
}

func (s *DefaultScheduler) Results() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make([]*Task, len(s.tasks))
	copy(results, s.tasks)
	return results
}

func (s *DefaultScheduler) Cancel() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *DefaultScheduler) GetTaskResults() <-chan *Task {
	return s.resultCh
}

func (s *DefaultScheduler) GetErrors() <-chan *errors.Error {
	return s.errorCh
}

type SimpleWorker struct {
	id        string
	capacity  int
	available atomic.Bool
	mu        sync.Mutex
}

func NewSimpleWorker(id string, capacity int) *SimpleWorker {
	w := &SimpleWorker{
		id:       id,
		capacity: capacity,
	}
	w.available.Store(true)
	return w
}

func (w *SimpleWorker) ID() string {
	return w.id
}

func (w *SimpleWorker) Capacity() int {
	return w.capacity
}

func (w *SimpleWorker) IsAvailable() bool {
	return w.available.Load()
}

func (w *SimpleWorker) Assign(ctx context.Context, task *Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.available.Load() {
		return errors.New(errors.ErrCodeServiceUnavailable, "worker not available")
	}

	w.available.Store(false)
	go func() {
		<-ctx.Done()
		w.available.Store(true)
	}()

	return nil
}

func (w *SimpleWorker) Heartbeat() error {
	return nil
}

type WorkerPool struct {
	workers    []*SimpleWorker
	mu         sync.RWMutex
	nextWorker int32
}

func NewWorkerPool(numWorkers int, capacityPerWorker int) *WorkerPool {
	workers := make([]*SimpleWorker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = NewSimpleWorker(generateWorkerID(i), capacityPerWorker)
	}

	return &WorkerPool{
		workers:    workers,
		nextWorker: 0,
	}
}

func (p *WorkerPool) GetWorker() *SimpleWorker {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := p.nextWorker
	p.nextWorker = (p.nextWorker + 1) % int32(len(p.workers))

	for i := 0; i < len(p.workers); i++ {
		worker := p.workers[(int(idx)+i)%len(p.workers)]
		if worker.IsAvailable() {
			return worker
		}
	}

	return p.workers[idx]
}

func (p *WorkerPool) GetAllWorkers() []Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()

	workers := make([]Worker, len(p.workers))
	for i, w := range p.workers {
		workers[i] = w
	}
	return workers
}

func (p *WorkerPool) AvailableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := 0
	for _, w := range p.workers {
		if w.IsAvailable() {
			count++
		}
	}
	return count
}

func generateTaskID(index int) string {
	return time.Now().Format("20060102150405") + "-" + string(rune('A'+index%26)) + "-" + string(rune('0'+index%10))
}

func generateWorkerID(index int) string {
	return "worker-" + time.Now().Format("20060102") + "-" + string(rune('a'+index))
}
