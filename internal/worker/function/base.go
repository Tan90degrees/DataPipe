package function

import (
	"context"
	"time"

	"datapipe/internal/models"
)

type ContextKey string

const (
	ContextKeyTaskID     ContextKey = "task_id"
	ContextKeyPipelineID ContextKey = "pipeline_id"
	ContextKeyNodeID     ContextKey = "node_id"
	ContextKeyWorkerID   ContextKey = "worker_id"
)

type ExecutionContext struct {
	TaskID     string
	PipelineID string
	NodeID     string
	WorkerID   string
	StartTime  time.Time
	Timeout    time.Duration
	Metadata   map[string]interface{}
}

func NewExecutionContext(taskID, pipelineID, nodeID, workerID string) *ExecutionContext {
	return &ExecutionContext{
		TaskID:     taskID,
		PipelineID: pipelineID,
		NodeID:     nodeID,
		WorkerID:   workerID,
		StartTime:  time.Now(),
		Timeout:    5 * time.Minute,
		Metadata:   make(map[string]interface{}),
	}
}

func (e *ExecutionContext) WithTimeout(timeout time.Duration) *ExecutionContext {
	e.Timeout = timeout
	return e
}

func (e *ExecutionContext) SetMetadata(key string, value interface{}) {
	e.Metadata[key] = value
}

func (e *ExecutionContext) GetMetadata(key string) (interface{}, bool) {
	val, ok := e.Metadata[key]
	return val, ok
}

type Function interface {
	GetName() string
	GetType() models.FunctionType
	GetVersion() string
	GetInputType() models.InputType
	GetOutputType() models.OutputType
	GetConfig() map[string]interface{}
	ValidateConfig() error
	Execute(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error)
	Initialize() error
	Destroy() error
}

type BaseFunction struct {
	name       string
	version    string
	inputType  models.InputType
	outputType models.OutputType
	config     map[string]interface{}
	functionType models.FunctionType
}

func NewBaseFunction(name, version string, fnType models.FunctionType, inputType models.InputType, outputType models.OutputType, config map[string]interface{}) *BaseFunction {
	return &BaseFunction{
		name:         name,
		version:      version,
		functionType: fnType,
		inputType:    inputType,
		outputType:   outputType,
		config:       config,
	}
}

func (b *BaseFunction) GetName() string {
	return b.name
}

func (b *BaseFunction) GetType() models.FunctionType {
	return b.functionType
}

func (b *BaseFunction) GetVersion() string {
	return b.version
}

func (b *BaseFunction) GetInputType() models.InputType {
	return b.inputType
}

func (b *BaseFunction) GetOutputType() models.OutputType {
	return b.outputType
}

func (b *BaseFunction) GetConfig() map[string]interface{} {
	return b.config
}

func (b *BaseFunction) ValidateConfig() error {
	return nil
}

func (b *BaseFunction) Initialize() error {
	return nil
}

func (b *BaseFunction) Destroy() error {
	return nil
}

type StartFunction interface {
	Function
	Scan(ctx context.Context, execCtx *ExecutionContext) ([]map[string]interface{}, error)
}

type NormalFunction interface {
	Function
	Process(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error)
}

type EndFunction interface {
	Function
	Finalize(ctx context.Context, execCtx *ExecutionContext, inputs []map[string]interface{}) (map[string]interface{}, error)
}

type FunctionFactory func(config map[string]interface{}) (Function, error)

var defaultFactory FunctionFactory = func(config map[string]interface{}) (Function, error) {
	return nil, nil
}

func RegisterFunctionFactory(factory FunctionFactory) {
	defaultFactory = factory
}

func CreateFunction(config map[string]interface{}) (Function, error) {
	return defaultFactory(config)
}
