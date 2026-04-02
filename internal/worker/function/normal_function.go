package function

import (
	"context"

	"datapipe/internal/models"
)

type NormalFunctionImpl struct {
	*BaseFunction
}

func NewNormalFunction(name, version string, config map[string]interface{}) *NormalFunctionImpl {
	base := NewBaseFunction(
		name,
		version,
		models.FunctionTypeNormal,
		models.InputType{Type: "object"},
		models.OutputType{Type: "object"},
		config,
	)

	return &NormalFunctionImpl{
		BaseFunction: base,
	}
}

func (n *NormalFunctionImpl) Process(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	return input, nil
}

func (n *NormalFunctionImpl) Execute(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	return n.Process(ctx, execCtx, input)
}

type DataProcessor interface {
	ProcessData(ctx context.Context, execCtx *ExecutionContext, data map[string]interface{}) (map[string]interface{}, error)
}

type NormalFunctionWithProcessor struct {
	*NormalFunctionImpl
	processor DataProcessor
}

func NewNormalFunctionWithProcessor(name, version string, config map[string]interface{}, processor DataProcessor) *NormalFunctionWithProcessor {
	fn := NewNormalFunction(name, version, config)
	return &NormalFunctionWithProcessor{
		NormalFunctionImpl: fn,
		processor:          processor,
	}
}

func (n *NormalFunctionWithProcessor) Process(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	if n.processor != nil {
		return n.processor.ProcessData(ctx, execCtx, input)
	}
	return input, nil
}
