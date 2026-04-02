package function

import (
	"context"

	"datapipe/internal/models"
)

type EndFunctionImpl struct {
	*BaseFunction
}

func NewEndFunction(name, version string, config map[string]interface{}) *EndFunctionImpl {
	base := NewBaseFunction(
		name,
		version,
		models.FunctionTypeEnd,
		models.InputType{Type: "array"},
		models.OutputType{Type: "object"},
		config,
	)

	return &EndFunctionImpl{
		BaseFunction: base,
	}
}

func (e *EndFunctionImpl) Finalize(ctx context.Context, execCtx *ExecutionContext, inputs []map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"processed_count": len(inputs),
		"results":          inputs,
	}, nil
}

func (e *EndFunctionImpl) Execute(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	inputs, ok := input["items"].([]map[string]interface{})
	if !ok {
		items := []map[string]interface{}{input}
		return e.Finalize(ctx, execCtx, items)
	}
	return e.Finalize(ctx, execCtx, inputs)
}

type ResultAggregator interface {
	Aggregate(ctx context.Context, execCtx *ExecutionContext, results []map[string]interface{}) (map[string]interface{}, error)
}

type EndFunctionWithAggregator struct {
	*EndFunctionImpl
	aggregator ResultAggregator
}

func NewEndFunctionWithAggregator(name, version string, config map[string]interface{}, aggregator ResultAggregator) *EndFunctionWithAggregator {
	fn := NewEndFunction(name, version, config)
	return &EndFunctionWithAggregator{
		EndFunctionImpl: fn,
		aggregator:      aggregator,
	}
}

func (e *EndFunctionWithAggregator) Finalize(ctx context.Context, execCtx *ExecutionContext, inputs []map[string]interface{}) (map[string]interface{}, error) {
	if e.aggregator != nil {
		return e.aggregator.Aggregate(ctx, execCtx, inputs)
	}
	return map[string]interface{}{
		"processed_count": len(inputs),
		"results":         inputs,
	}, nil
}
