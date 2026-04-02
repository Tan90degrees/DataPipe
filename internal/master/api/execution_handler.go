package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"datapipe/internal/common/errors"
	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/repository"
)

type ExecutionHandler struct {
	repo *repository.ExecutionRepository
}

func NewExecutionHandler(db *gorm.DB) *ExecutionHandler {
	return &ExecutionHandler{
		repo: repository.NewExecutionRepository(db),
	}
}

type ExecutionListResponse struct {
	Data   []*models.Execution `json:"data"`
	Total  int64               `json:"total"`
	Offset int                 `json:"offset"`
	Limit  int                 `json:"limit"`
}

type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	TaskID    string                 `json:"task_id,omitempty"`
	NodeID    string                 `json:"node_id,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type ExecutionMetrics struct {
	ExecutionID    string                 `json:"execution_id"`
	TotalTasks     int                    `json:"total_tasks"`
	CompletedTasks int                    `json:"completed_tasks"`
	FailedTasks    int                    `json:"failed_tasks"`
	PendingTasks   int                    `json:"pending_tasks"`
	RunningTasks   int                    `json:"running_tasks"`
	Progress       int                    `json:"progress"`
	Duration       int64                  `json:"duration_seconds"`
	StartTime      *time.Time             `json:"start_time,omitempty"`
	EndTime        *time.Time             `json:"end_time,omitempty"`
	TaskMetrics    map[string]TaskMetrics `json:"task_metrics,omitempty"`
}

type TaskMetrics struct {
	TaskID       string     `json:"task_id"`
	NodeID       string     `json:"node_id"`
	FunctionName string     `json:"function_name"`
	Status       string     `json:"status"`
	Duration     int64      `json:"duration_seconds"`
	RetryCount   int        `json:"retry_count"`
}

func (h *ExecutionHandler) List(c *gin.Context) {
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	pipelineID := c.Query("pipeline_id")
	status := c.Query("status")

	if limit > 100 {
		limit = 100
	}

	var executions []*models.Execution
	var total int64
	var err error

	if pipelineID != "" {
		executions, total, err = h.repo.ListByPipelineID(c.Request.Context(), pipelineID, offset, limit)
	} else if status != "" {
		executions, total, err = h.repo.ListByStatus(c.Request.Context(), models.ExecutionStatus(status), offset, limit)
	} else {
		executions, total, err = h.repo.List(c.Request.Context(), offset, limit)
	}

	if err != nil {
		logging.Error(fmt.Sprintf("Failed to list executions: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to list executions"))
		return
	}

	c.JSON(http.StatusOK, ExecutionListResponse{
		Data:   executions,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	})
}

func (h *ExecutionHandler) Get(c *gin.Context) {
	id := c.Param("id")

	execution, err := h.repo.GetByIDWithTasks(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Execution not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get execution: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get execution"))
		return
	}

	c.JSON(http.StatusOK, execution)
}

func (h *ExecutionHandler) Logs(c *gin.Context) {
	id := c.Param("id")

	_, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Execution not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get execution: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get execution"))
		return
	}

	tasks, err := h.repo.GetTasksByExecutionID(c.Request.Context(), id)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to get execution tasks: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get execution tasks"))
		return
	}

	logs := make([]LogEntry, 0)
	for _, task := range tasks {
		logs = append(logs, LogEntry{
			Timestamp: task.CreatedAt,
			Level:     "INFO",
			Message:   "Task created",
			TaskID:    task.ID,
			NodeID:    task.NodeID,
		})

		if task.StartTime != nil {
			logs = append(logs, LogEntry{
				Timestamp: *task.StartTime,
				Level:     "INFO",
				Message:   "Task started",
				TaskID:    task.ID,
				NodeID:    task.NodeID,
				Metadata: map[string]interface{}{
					"worker_id": task.WorkerID,
				},
			})
		}

		if task.EndTime != nil {
			level := "INFO"
			if task.Status == models.TaskStatusFailed {
				level = "ERROR"
			}
			logs = append(logs, LogEntry{
				Timestamp: *task.EndTime,
				Level:     level,
				Message:   "Task completed",
				TaskID:    task.ID,
				NodeID:    task.NodeID,
				Metadata: map[string]interface{}{
					"status":        task.Status,
					"error_message": task.ErrorMessage,
				},
			})
		}

		if task.ErrorMessage != "" {
			logs = append(logs, LogEntry{
				Timestamp: task.CreatedAt,
				Level:     "ERROR",
				Message:   task.ErrorMessage,
				TaskID:    task.ID,
				NodeID:    task.NodeID,
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"execution_id": id,
		"logs":         logs,
	})
}

func (h *ExecutionHandler) Metrics(c *gin.Context) {
	id := c.Param("id")

	execution, err := h.repo.GetByIDWithTasks(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Execution not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get execution: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get execution"))
		return
	}

	tasks, err := h.repo.GetTasksByExecutionID(c.Request.Context(), id)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to get execution tasks: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get execution tasks"))
		return
	}

	taskMetrics := make(map[string]TaskMetrics)
	totalTasks := len(tasks)
	completedTasks := 0
	failedTasks := 0
	pendingTasks := 0
	runningTasks := 0

	for _, task := range tasks {
		var duration int64
		if task.StartTime != nil && task.EndTime != nil {
			duration = int64(task.EndTime.Sub(*task.StartTime).Seconds())
		}

		taskMetrics[task.ID] = TaskMetrics{
			TaskID:       task.ID,
			NodeID:       task.NodeID,
			FunctionName: task.FunctionName,
			Status:       string(task.Status),
			Duration:     duration,
			RetryCount:   task.RetryCount,
		}

		switch task.Status {
		case models.TaskStatusCompleted:
			completedTasks++
		case models.TaskStatusFailed:
			failedTasks++
		case models.TaskStatusPending:
			pendingTasks++
		case models.TaskStatusRunning, models.TaskStatusScheduled:
			runningTasks++
		}
	}

	progress := 0
	if totalTasks > 0 {
		progress = (completedTasks * 100) / totalTasks
	}

	var duration int64
	if execution.StartTime != nil {
		if execution.EndTime != nil {
			duration = int64(execution.EndTime.Sub(*execution.StartTime).Seconds())
		} else {
			duration = int64(time.Now().Sub(*execution.StartTime).Seconds())
		}
	}

	metrics := ExecutionMetrics{
		ExecutionID:    execution.ID,
		TotalTasks:     totalTasks,
		CompletedTasks: completedTasks,
		FailedTasks:    failedTasks,
		PendingTasks:   pendingTasks,
		RunningTasks:   runningTasks,
		Progress:       progress,
		Duration:       duration,
		StartTime:      execution.StartTime,
		EndTime:        execution.EndTime,
		TaskMetrics:    taskMetrics,
	}

	c.JSON(http.StatusOK, metrics)
}
