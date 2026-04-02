package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datapipe/internal/models"
	"datapipe/internal/worker/function"
)

type Runtime struct {
	mu          sync.RWMutex
	registry    *function.Registry
	contexts    map[string]*function.ExecutionContext
	maxContexts int
	timeout     time.Duration
}

func NewRuntime(registry *function.Registry) *Runtime {
	return &Runtime{
		registry:    registry,
		contexts:    make(map[string]*function.ExecutionContext),
		maxContexts: 1000,
		timeout:     5 * time.Minute,
	}
}

func (r *Runtime) SetMaxContexts(max int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxContexts = max
}

func (r *Runtime) SetDefaultTimeout(timeout time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeout = timeout
}

func (r *Runtime) CreateContext(taskID, pipelineID, nodeID, workerID string) (*function.ExecutionContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.contexts) >= r.maxContexts {
		return nil, fmt.Errorf("max contexts limit reached: %d", r.maxContexts)
	}

	ctx := function.NewExecutionContext(taskID, pipelineID, nodeID, workerID)
	ctx.Timeout = r.timeout
	r.contexts[taskID] = ctx

	return ctx, nil
}

func (r *Runtime) GetContext(taskID string) (*function.ExecutionContext, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx, ok := r.contexts[taskID]
	return ctx, ok
}

func (r *Runtime) RemoveContext(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.contexts, taskID)
}

func (r *Runtime) ExecuteFunction(ctx context.Context, fn function.Function, execCtx *function.ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	if execCtx == nil {
		return nil, fmt.Errorf("execution context is nil")
	}

	if err := fn.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	if err := fn.Initialize(); err != nil {
		return nil, fmt.Errorf("function initialization failed: %w", err)
	}

	defer func() {
		if err := fn.Destroy(); err != nil {
		}
	}()

	return fn.Execute(ctx, execCtx, input)
}

func (r *Runtime) ExecuteWithTimeout(ctx context.Context, fn function.Function, execCtx *function.ExecutionContext, input map[string]interface{}, timeout time.Duration) (map[string]interface{}, error) {
	type result struct {
		output map[string]interface{}
		err    error
	}

	resultCh := make(chan result, 1)

	go func() {
		output, err := r.ExecuteFunction(ctx, fn, execCtx, input)
		resultCh <- result{output: output, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resultCh:
		return res.output, res.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("function execution timed out after %v", timeout)
	}
}

func (r *Runtime) LoadFunction(fnDef *models.Function) (function.Function, error) {
	if r.registry == nil {
		return nil, fmt.Errorf("function registry is nil")
	}

	fn, err := r.registry.Get(fnDef.Name, fnDef.Version)
	if err == nil {
		return fn, nil
	}

	return nil, fmt.Errorf("function not found: %s:%s", fnDef.Name, fnDef.Version)
}

func (r *Runtime) RegisterFunction(fn function.Function) error {
	if r.registry == nil {
		return fmt.Errorf("function registry is nil")
	}
	return r.registry.Register(fn)
}

func (r *Runtime) ListFunctions() []function.Function {
	if r.registry == nil {
		return nil
	}
	return r.registry.List()
}

func (r *Runtime) GetFunction(name, version string) (function.Function, error) {
	if r.registry == nil {
		return nil, fmt.Errorf("function registry is nil")
	}
	return r.registry.Get(name, version)
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.contexts = make(map[string]*function.ExecutionContext)
	return nil
}

func (r *Runtime) CleanupContext(taskID string) {
	r.RemoveContext(taskID)
}

type FunctionLoader interface {
	Load(fnDef *models.Function) (function.Function, error)
	Unload(name, version string) error
}

type defaultFunctionLoader struct {
	registry *function.Registry
}

func NewFunctionLoader(registry *function.Registry) FunctionLoader {
	return &defaultFunctionLoader{
		registry: registry,
	}
}

func (l *defaultFunctionLoader) Load(fnDef *models.Function) (function.Function, error) {
	return l.registry.Get(fnDef.Name, fnDef.Version)
}

func (l *defaultFunctionLoader) Unload(name, version string) error {
	return l.registry.Unregister(name, version)
}
