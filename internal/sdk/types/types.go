package types

import (
	"encoding/json"
	"time"
)

type DataType string

const (
	DataTypeAny     DataType = "any"
	DataTypeString  DataType = "string"
	DataTypeInteger DataType = "integer"
	DataTypeFloat   DataType = "float"
	DataTypeBoolean DataType = "boolean"
	DataTypeArray   DataType = "array"
	DataTypeObject  DataType = "object"
	DataTypeFile    DataType = "file"
	DataTypeBinary  DataType = "binary"
)

type FileContent struct {
	Data      []byte `json:"data"`
	Encoding  string `json:"encoding"`
}

type FileInfo struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	Path        string `json:"path"`
	Checksum    string `json:"checksum,omitempty"`
}

type DataRecord struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	DataType  DataType        `json:"data_type"`
	Value     json.RawMessage `json:"value"`
	Metadata  map[string]any  `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type InputDefinition struct {
	Name     string   `json:"name"`
	DataType DataType `json:"data_type"`
	Required bool     `json:"required"`
	Default  any      `json:"default,omitempty"`
}

type OutputDefinition struct {
	Name        string   `json:"name"`
	DataType    DataType `json:"data_type"`
	Description string   `json:"description"`
}

type FunctionDefinition struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Version     string              `json:"version"`
	Inputs      []InputDefinition   `json:"inputs"`
	Outputs     []OutputDefinition  `json:"outputs"`
	Parameters  map[string]any      `json:"parameters"`
	Timeout     int                 `json:"timeout"`
	RetryCount  int                 `json:"retry_count"`
}

type NodeDefinition struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	FunctionName      string                 `json:"function_name"`
	FunctionVersion   string                 `json:"function_version,omitempty"`
	InputMappings     map[string]string      `json:"input_mappings"`
	OutputMappings    map[string]string      `json:"output_mappings"`
	Parameters        map[string]any         `json:"parameters"`
	RetryCount        int                    `json:"retry_count"`
	Timeout           int                    `json:"timeout"`
	Condition         string                 `json:"condition,omitempty"`
}

type PipelineDefinition struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Version     string                 `json:"version"`
	Nodes       []NodeDefinition       `json:"nodes"`
	Edges       []EdgeDefinition       `json:"edges"`
	Parameters  map[string]any         `json:"parameters"`
	Metadata    map[string]any         `json:"metadata"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

type EdgeDefinition struct {
	SourceNodeID    string `json:"source_node_id"`
	SourceOutput    string `json:"source_output"`
	TargetNodeID    string `json:"target_node_id"`
	TargetInput     string `json:"target_input"`
}

type ExecutionContext struct {
	ExecutionID string                 `json:"execution_id"`
	PipelineID  string                 `json:"pipeline_id"`
	NodeID      string                 `json:"node_id"`
	Parameters  map[string]any         `json:"parameters"`
	Inputs      map[string]DataRecord  `json:"inputs"`
	Outputs     map[string]DataRecord  `json:"outputs"`
	Metadata    map[string]any         `json:"metadata"`
	StartTime   time.Time              `json:"start_time"`
	EndTime     *time.Time             `json:"end_time,omitempty"`
}

type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusSuccess   ExecutionStatus = "success"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCancelled ExecutionStatus = "cancelled"
)

type Execution struct {
	ID          string           `json:"id"`
	PipelineID  string           `json:"pipeline_id"`
	Status      ExecutionStatus  `json:"status"`
	Context     ExecutionContext `json:"context"`
	Error       string           `json:"error,omitempty"`
	StartedAt   time.Time        `json:"started_at"`
	CompletedAt *time.Time       `json:"completed_at,omitempty"`
}
