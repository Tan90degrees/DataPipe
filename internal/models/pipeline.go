package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type PipelineStatus string

const (
	PipelineStatusDraft   PipelineStatus = "draft"
	PipelineStatusActive  PipelineStatus = "active"
	PipelineStatusPaused  PipelineStatus = "paused"
	PipelineStatusStopped PipelineStatus = "stopped"
)

type Definition struct {
	Nodes      []NodeDefinition `json:"nodes"`
	Edges      []EdgeDefinition  `json:"edges"`
	Execution  ExecutionConfig  `json:"execution"`
}

type NodeDefinition struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Function string                 `json:"function"`
	Config   map[string]interface{} `json:"config"`
	DependsOn []string              `json:"depends_on,omitempty"`
}

type EdgeDefinition struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type ExecutionConfig struct {
	Parallelism int           `json:"parallelism"`
	RetryPolicy RetryPolicy   `json:"retryPolicy"`
}

type RetryPolicy struct {
	MaxRetries int    `json:"maxRetries"`
	Backoff    string `json:"backoff"`
}

func (d Definition) Value() (driver.Value, error) {
	return json.Marshal(d)
}

func (d *Definition) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, d)
}

type Pipeline struct {
	ID          string         `gorm:"type:varchar(36);primaryKey" json:"id"`
	Name        string         `gorm:"type:varchar(255);uniqueIndex;not null" json:"name"`
	Description string         `gorm:"type:text" json:"description"`
	Definition  Definition     `gorm:"type:json;not null" json:"definition"`
	Version     int            `gorm:"not null;default:1" json:"version"`
	Status      PipelineStatus `gorm:"type:varchar(20);default:'draft'" json:"status"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	CreatedBy   string         `gorm:"type:varchar(255)" json:"created_by"`
}

func (Pipeline) TableName() string {
	return "pipelines"
}

type PipelineVersion struct {
	ID          string     `gorm:"type:varchar(36);primaryKey" json:"id"`
	PipelineID  string     `gorm:"type:varchar(36);not null;index:idx_pipeline_version" json:"pipeline_id"`
	Version     int        `gorm:"not null" json:"version"`
	Definition  Definition `gorm:"type:json;not null" json:"definition"`
	Changelog   string     `gorm:"type:text" json:"changelog"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	CreatedBy   string     `gorm:"type:varchar(255)" json:"created_by"`
}

func (PipelineVersion) TableName() string {
	return "pipeline_versions"
}
