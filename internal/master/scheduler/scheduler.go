package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/repository"
)

type Scheduler struct {
	db           *gorm.DB
	execRepo     *repository.ExecutionRepository
	pipelineRepo *repository.PipelineRepository
	tasks        chan *TaskAssignment
	workers      map[string]*WorkerInfo
	mu           sync.RWMutex
	running      bool
	stopCh       chan struct{}
	logger       logging.Logger
}

type TaskAssignment struct {
	ExecutionID  string
	Task         *models.Task
	PipelineDef  *models.Definition
}

type WorkerInfo struct {
	ID        string
	Host      string
	Port      int
	Status    string
	Load      int
	UpdatedAt time.Time
}

func NewScheduler(db *gorm.DB) *Scheduler {
	return &Scheduler{
		db:           db,
		execRepo:     repository.NewExecutionRepository(db),
		pipelineRepo: repository.NewPipelineRepository(db),
		tasks:        make(chan *TaskAssignment, 1000),
		workers:      make(map[string]*WorkerInfo),
		stopCh:       make(chan struct{}),
		logger:       logging.New("scheduler"),
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("scheduler already running")
	}
	s.running = true
	s.mu.Unlock()

	go s.processTasks()
	go s.cleanupStaleTasks()

	s.logger.Info("Scheduler started")
	return nil
}

func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	close(s.stopCh)

	s.logger.Info("Scheduler stopped")
	return nil
}

func (s *Scheduler) processTasks() {
	for {
		select {
		case <-s.stopCh:
			return
		case assignment := <-s.tasks:
			s.executeTask(assignment)
		}
	}
}

func (s *Scheduler) executeTask(assignment *TaskAssignment) {
	s.logger.Info(fmt.Sprintf("Executing task: %s for execution: %s", assignment.Task.ID, assignment.ExecutionID))

	if err := s.execRepo.UpdateTaskStatus(context.Background(), assignment.Task.ID, models.TaskStatusRunning); err != nil {
		s.logger.Error(fmt.Sprintf("Failed to update task status: %v", err))
		return
	}

	time.Sleep(100 * time.Millisecond)

	if err := s.execRepo.UpdateTaskStatus(context.Background(), assignment.Task.ID, models.TaskStatusCompleted); err != nil {
		s.logger.Error(fmt.Sprintf("Failed to update task status to completed: %v", err))
		return
	}

	s.checkExecutionCompletion(assignment.ExecutionID)
}

func (s *Scheduler) checkExecutionCompletion(executionID string) {
	execution, err := s.execRepo.GetByIDWithTasks(context.Background(), executionID)
	if err != nil {
		s.logger.Error(fmt.Sprintf("Failed to get execution: %v", err))
		return
	}

	allCompleted := true
	for _, task := range execution.Tasks {
		if task.Status != models.TaskStatusCompleted && task.Status != models.TaskStatusFailed {
			allCompleted = false
			break
		}
	}

	if allCompleted {
		hasFailed := false
		for _, task := range execution.Tasks {
			if task.Status == models.TaskStatusFailed {
				hasFailed = true
				break
			}
		}

		status := models.ExecutionStatusCompleted
		if hasFailed {
			status = models.ExecutionStatusFailed
		}

		if err := s.execRepo.UpdateStatus(context.Background(), executionID, status); err != nil {
			s.logger.Error(fmt.Sprintf("Failed to update execution status: %v", err))
		}

		s.logger.Info(fmt.Sprintf("Execution %s completed with status: %s", executionID, status))
	}
}

func (s *Scheduler) cleanupStaleTasks() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.handleStaleTasks()
		}
	}
}

func (s *Scheduler) handleStaleTasks() {
	s.logger.Debug("Checking for stale tasks...")
}

func (s *Scheduler) ScheduleExecution(ctx context.Context, pipelineID string) (*models.Execution, error) {
	pipeline, err := s.pipelineRepo.GetByID(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	execution := &models.Execution{
		ID:              uuid.New().String(),
		PipelineID:      pipelineID,
		PipelineVersion: pipeline.Version,
		Status:          models.ExecutionStatusPending,
	}

	if err := s.execRepo.Create(ctx, execution); err != nil {
		return nil, fmt.Errorf("failed to create execution: %w", err)
	}

	if err := s.createTasks(ctx, execution, &pipeline.Definition); err != nil {
		return nil, fmt.Errorf("failed to create tasks: %w", err)
	}

	if err := s.execRepo.UpdateStatus(ctx, execution.ID, models.ExecutionStatusRunning); err != nil {
		s.logger.Error(fmt.Sprintf("Failed to update execution status: %v", err))
	}

	s.scheduleTasks(execution)

	return execution, nil
}

func (s *Scheduler) createTasks(ctx context.Context, execution *models.Execution, def *models.Definition) error {
	sortedNodes, err := s.topologicalSort(def.Nodes, def.Edges)
	if err != nil {
		return fmt.Errorf("failed to topological sort: %w", err)
	}
	for _, nodeID := range sortedNodes {
		var nodeDef models.NodeDefinition
		for _, n := range def.Nodes {
			if n.ID == nodeID {
				nodeDef = n
				break
			}
		}

		task := &models.Task{
			ID:           uuid.New().String(),
			ExecutionID:  execution.ID,
			NodeID:       nodeDef.ID,
			FunctionName: nodeDef.Function,
			Status:       models.TaskStatusPending,
		}

		if err := s.execRepo.CreateTask(ctx, task); err != nil {
			return fmt.Errorf("failed to create task: %w", err)
		}
	}

	return nil
}

func (s *Scheduler) scheduleTasks(execution *models.Execution) {
	tasks, err := s.execRepo.GetTasksByExecutionID(context.Background(), execution.ID)
	if err != nil {
		s.logger.Error(fmt.Sprintf("Failed to get tasks for scheduling: %v", err))
		return
	}

	go func() {
		for _, task := range tasks {
			if task.Status != models.TaskStatusPending {
				continue
			}

			pipeline, err := s.pipelineRepo.GetByID(context.Background(), execution.PipelineID)
			if err != nil {
				s.logger.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
				continue
			}

			assignment := &TaskAssignment{
				ExecutionID: execution.ID,
				Task:        task,
				PipelineDef: &pipeline.Definition,
			}

			select {
			case s.tasks <- assignment:
				s.logger.Debug(fmt.Sprintf("Task scheduled: %s", task.ID))
			default:
				s.logger.Warn(fmt.Sprintf("Task queue full, task %s not scheduled", task.ID))
			}
		}
	}()
}

func (s *Scheduler) topologicalSort(nodes []models.NodeDefinition, edges []models.EdgeDefinition) ([]string, error) {
	adjList := make(map[string][]string)
	inDegree := make(map[string]int)

	for _, node := range nodes {
		adjList[node.ID] = []string{}
		inDegree[node.ID] = 0
	}

	for _, edge := range edges {
		adjList[edge.From] = append(adjList[edge.From], edge.To)
		inDegree[edge.To]++
	}

	for _, node := range nodes {
		if node.DependsOn != nil {
			for _, dep := range node.DependsOn {
				adjList[dep] = append(adjList[dep], node.ID)
				inDegree[node.ID]++
			}
		}
	}

	var queue []string
	for _, node := range nodes {
		if inDegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}

	var result []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		for _, neighbor := range adjList[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(nodes) {
		return nil, fmt.Errorf("cycle detected in pipeline graph")
	}

	return result, nil
}

func (s *Scheduler) RegisterWorker(worker *WorkerInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker.UpdatedAt = time.Now()
	s.workers[worker.ID] = worker

	s.logger.Info(fmt.Sprintf("Worker registered: %s", worker.ID))
}

func (s *Scheduler) UnregisterWorker(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.workers, workerID)

	s.logger.Info(fmt.Sprintf("Worker unregistered: %s", workerID))
}

func (s *Scheduler) GetAvailableWorkers() []*WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	available := make([]*WorkerInfo, 0)
	for _, worker := range s.workers {
		if worker.Status == "online" && worker.Load < 100 {
			available = append(available, worker)
		}
	}

	return available
}

func (s *Scheduler) UpdateWorkerLoad(workerID string, load int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		worker.Load = load
		worker.UpdatedAt = time.Now()
	}
}
