package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"datapipe/internal/models"
)

type ExecutionRepository struct {
	db *gorm.DB
}

func NewExecutionRepository(db *gorm.DB) *ExecutionRepository {
	return &ExecutionRepository{db: db}
}

func (r *ExecutionRepository) Create(ctx context.Context, execution *models.Execution) error {
	if execution.ID == "" {
		execution.ID = uuid.New().String()
	}
	if execution.Status == "" {
		execution.Status = models.ExecutionStatusPending
	}
	return r.db.WithContext(ctx).Create(execution).Error
}

func (r *ExecutionRepository) GetByID(ctx context.Context, id string) (*models.Execution, error) {
	var execution models.Execution
	err := r.db.WithContext(ctx).First(&execution, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &execution, nil
}

func (r *ExecutionRepository) GetByIDWithTasks(ctx context.Context, id string) (*models.Execution, error) {
	var execution models.Execution
	err := r.db.WithContext(ctx).Preload("Tasks").First(&execution, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &execution, nil
}

func (r *ExecutionRepository) List(ctx context.Context, offset, limit int) ([]*models.Execution, int64, error) {
	var executions []*models.Execution
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Execution{})

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&executions).Error; err != nil {
		return nil, 0, err
	}

	return executions, total, nil
}

func (r *ExecutionRepository) ListByPipelineID(ctx context.Context, pipelineID string, offset, limit int) ([]*models.Execution, int64, error) {
	var executions []*models.Execution
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Execution{}).Where("pipeline_id = ?", pipelineID)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&executions).Error; err != nil {
		return nil, 0, err
	}

	return executions, total, nil
}

func (r *ExecutionRepository) ListByStatus(ctx context.Context, status models.ExecutionStatus, offset, limit int) ([]*models.Execution, int64, error) {
	var executions []*models.Execution
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Execution{}).Where("status = ?", status)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&executions).Error; err != nil {
		return nil, 0, err
	}

	return executions, total, nil
}

func (r *ExecutionRepository) Update(ctx context.Context, execution *models.Execution) error {
	return r.db.WithContext(ctx).Save(execution).Error
}

func (r *ExecutionRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("execution_id = ?", id).Delete(&models.Task{}).Error; err != nil {
			return err
		}
		return tx.Delete(&models.Execution{}, "id = ?", id).Error
	})
}

func (r *ExecutionRepository) UpdateStatus(ctx context.Context, id string, status models.ExecutionStatus) error {
	updates := map[string]interface{}{
		"status": status,
	}

	now := time.Now()
	switch status {
	case models.ExecutionStatusRunning:
		updates["start_time"] = now
	case models.ExecutionStatusCompleted, models.ExecutionStatusFailed, models.ExecutionStatusCancelled:
		updates["end_time"] = now
	}

	return r.db.WithContext(ctx).Model(&models.Execution{}).Where("id = ?", id).Updates(updates).Error
}

func (r *ExecutionRepository) UpdateProgress(ctx context.Context, id string, progress int) error {
	return r.db.WithContext(ctx).Model(&models.Execution{}).Where("id = ?", id).Update("progress", progress).Error
}

func (r *ExecutionRepository) SetError(ctx context.Context, id string, errorMessage string) error {
	updates := map[string]interface{}{
		"status":        models.ExecutionStatusFailed,
		"error_message": errorMessage,
		"end_time":      time.Now(),
	}
	return r.db.WithContext(ctx).Model(&models.Execution{}).Where("id = ?", id).Updates(updates).Error
}

func (r *ExecutionRepository) CreateTask(ctx context.Context, task *models.Task) error {
	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.Status == "" {
		task.Status = models.TaskStatusPending
	}
	if task.RetryCount == 0 {
		task.RetryCount = 0
	}
	return r.db.WithContext(ctx).Create(task).Error
}

func (r *ExecutionRepository) GetTaskByID(ctx context.Context, id string) (*models.Task, error) {
	var task models.Task
	err := r.db.WithContext(ctx).First(&task, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (r *ExecutionRepository) GetTasksByExecutionID(ctx context.Context, executionID string) ([]*models.Task, error) {
	var tasks []*models.Task
	err := r.db.WithContext(ctx).
		Where("execution_id = ?", executionID).
		Order("created_at ASC").
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (r *ExecutionRepository) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus) error {
	updates := map[string]interface{}{
		"status": status,
	}

	now := time.Now()
	switch status {
	case models.TaskStatusScheduled:
		updates["start_time"] = now
	case models.TaskStatusRunning:
		if updates["start_time"] == nil {
			updates["start_time"] = now
		}
	case models.TaskStatusCompleted, models.TaskStatusFailed:
		updates["end_time"] = now
	}

	return r.db.WithContext(ctx).Model(&models.Task{}).Where("id = ?", taskID).Updates(updates).Error
}

func (r *ExecutionRepository) UpdateTaskWorker(ctx context.Context, taskID string, workerID string) error {
	return r.db.WithContext(ctx).Model(&models.Task{}).Where("id = ?", taskID).Update("worker_id", workerID).Error
}

func (r *ExecutionRepository) UpdateTaskOutput(ctx context.Context, taskID string, outputData models.JSONMap) error {
	return r.db.WithContext(ctx).Model(&models.Task{}).Where("id = ?", taskID).Update("output_data", outputData).Error
}

func (r *ExecutionRepository) SetTaskError(ctx context.Context, taskID string, errorMessage string) error {
	updates := map[string]interface{}{
		"status":        models.TaskStatusFailed,
		"error_message": errorMessage,
		"end_time":      time.Now(),
	}
	return r.db.WithContext(ctx).Model(&models.Task{}).Where("id = ?", taskID).Updates(updates).Error
}

func (r *ExecutionRepository) IncrementTaskRetry(ctx context.Context, taskID string) error {
	return r.db.WithContext(ctx).Model(&models.Task{}).Where("id = ?", taskID).UpdateColumn("retry_count", gorm.Expr("retry_count + 1")).Error
}

func (r *ExecutionRepository) GetPendingExecutions(ctx context.Context, limit int) ([]*models.Execution, error) {
	var executions []*models.Execution
	err := r.db.WithContext(ctx).
		Where("status = ?", models.ExecutionStatusPending).
		Order("created_at ASC").
		Limit(limit).
		Find(&executions).Error
	if err != nil {
		return nil, err
	}
	return executions, nil
}

func (r *ExecutionRepository) CountByStatus(ctx context.Context) (map[models.ExecutionStatus]int64, error) {
	type StatusCount struct {
		Status models.ExecutionStatus
		Count  int64
	}

	var results []StatusCount
	err := r.db.WithContext(ctx).
		Model(&models.Execution{}).
		Select("status, count(*) as count").
		Group("status").
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	countMap := make(map[models.ExecutionStatus]int64)
	for _, rc := range results {
		countMap[rc.Status] = rc.Count
	}

	return countMap, nil
}
