package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning    ExecutionStatus = "running"
	ExecutionStatusCompleted  ExecutionStatus = "completed"
	ExecutionStatusFailed     ExecutionStatus = "failed"
	ExecutionStatusCancelled  ExecutionStatus = "cancelled"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusScheduled TaskStatus = "scheduled"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
)

type Execution struct {
	ID              string         `gorm:"type:varchar(36);primaryKey" json:"id"`
	PipelineID      string         `gorm:"type:varchar(36);not null;index:idx_pipeline_id" json:"pipeline_id"`
	PipelineVersion int            `gorm:"not null" json:"pipeline_version"`
	Status          ExecutionStatus `gorm:"type:varchar(20);default:'pending';index:idx_status" json:"status"`
	StartTime       *time.Time     `json:"start_time,omitempty"`
	EndTime         *time.Time     `json:"end_time,omitempty"`
	Progress        int            `gorm:"default:0" json:"progress"`
	ErrorMessage    string         `gorm:"type:text" json:"error_message,omitempty"`
	CreatedAt       time.Time      `gorm:"autoCreateTime;index:idx_created_at" json:"created_at"`
	Tasks           []Task         `gorm:"foreignKey:ExecutionID" json:"tasks,omitempty"`
}

func (Execution) TableName() string {
	return "executions"
}

type Task struct {
	ID           string     `gorm:"type:varchar(36);primaryKey" json:"id"`
	ExecutionID  string     `gorm:"type:varchar(36);not null;index:idx_execution_id" json:"execution_id"`
	NodeID       string     `gorm:"type:varchar(255);not null" json:"node_id"`
	FunctionName string     `gorm:"type:varchar(255);not null" json:"function_name"`
	Status       TaskStatus `gorm:"type:varchar(20);default:'pending';index:idx_task_status" json:"status"`
	WorkerID     string     `gorm:"type:varchar(255)" json:"worker_id,omitempty"`
	StartTime    *time.Time `json:"start_time,omitempty"`
	EndTime      *time.Time `json:"end_time,omitempty"`
	InputData    JSONMap    `gorm:"type:json" json:"input_data,omitempty"`
	OutputData   JSONMap    `gorm:"type:json" json:"output_data,omitempty"`
	ErrorMessage string     `gorm:"type:text" json:"error_message,omitempty"`
	RetryCount   int        `gorm:"default:0" json:"retry_count"`
	CreatedAt    time.Time  `gorm:"autoCreateTime" json:"created_at"`
}

func (Task) TableName() string {
	return "tasks"
}

type JSONMap map[string]interface{}

func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

func (j *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, j)
}
