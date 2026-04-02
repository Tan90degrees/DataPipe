package function

import (
	"fmt"
	"sync"

	"datapipe/internal/models"
)

type Registry struct {
	mu         sync.RWMutex
	functions  map[string]Function
	startFuncs map[string]Function
	normalFuncs map[string]Function
	endFuncs   map[string]Function
}

var (
	globalRegistry *Registry
	registryOnce   sync.Once
)

func NewRegistry() *Registry {
	return &Registry{
		functions:   make(map[string]Function),
		startFuncs:  make(map[string]Function),
		normalFuncs: make(map[string]Function),
		endFuncs:    make(map[string]Function),
	}
}

func GetRegistry() *Registry {
	if globalRegistry == nil {
		registryOnce.Do(func() {
			globalRegistry = NewRegistry()
		})
	}
	return globalRegistry
}

func (r *Registry) Register(fn Function) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fn == nil {
		return fmt.Errorf("function cannot be nil")
	}

	name := fn.GetName()
	if name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	key := r.makeKey(name, fn.GetVersion())

	switch fn.GetType() {
	case models.FunctionTypeStart:
		r.startFuncs[key] = fn
	case models.FunctionTypeNormal:
		r.normalFuncs[key] = fn
	case models.FunctionTypeEnd:
		r.endFuncs[key] = fn
	default:
		return fmt.Errorf("unknown function type: %s", fn.GetType())
	}

	r.functions[key] = fn
	return nil
}

func (r *Registry) Unregister(name, version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.makeKey(name, version)

	delete(r.functions, key)
	delete(r.startFuncs, key)
	delete(r.normalFuncs, key)
	delete(r.endFuncs, key)

	return nil
}

func (r *Registry) Get(name, version string) (Function, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.makeKey(name, version)

	fn, ok := r.functions[key]
	if !ok {
		return nil, fmt.Errorf("function not found: %s:%s", name, version)
	}

	return fn, nil
}

func (r *Registry) GetByType(fnType models.FunctionType, name, version string) (Function, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.makeKey(name, version)

	switch fnType {
	case models.FunctionTypeStart:
		fn, ok := r.startFuncs[key]
		if !ok {
			return nil, fmt.Errorf("start function not found: %s:%s", name, version)
		}
		return fn, nil
	case models.FunctionTypeNormal:
		fn, ok := r.normalFuncs[key]
		if !ok {
			return nil, fmt.Errorf("normal function not found: %s:%s", name, version)
		}
		return fn, nil
	case models.FunctionTypeEnd:
		fn, ok := r.endFuncs[key]
		if !ok {
			return nil, fmt.Errorf("end function not found: %s:%s", name, version)
		}
		return fn, nil
	default:
		return nil, fmt.Errorf("unknown function type: %s", fnType)
	}
}

func (r *Registry) List() []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Function, 0, len(r.functions))
	for _, fn := range r.functions {
		result = append(result, fn)
	}
	return result
}

func (r *Registry) ListByType(fnType models.FunctionType) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var funcs map[string]Function
	switch fnType {
	case models.FunctionTypeStart:
		funcs = r.startFuncs
	case models.FunctionTypeNormal:
		funcs = r.normalFuncs
	case models.FunctionTypeEnd:
		funcs = r.endFuncs
	default:
		return nil
	}

	result := make([]Function, 0, len(funcs))
	for _, fn := range funcs {
		result = append(result, fn)
	}
	return result
}

func (r *Registry) makeKey(name, version string) string {
	return fmt.Sprintf("%s:%s", name, version)
}

func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.functions = make(map[string]Function)
	r.startFuncs = make(map[string]Function)
	r.normalFuncs = make(map[string]Function)
	r.endFuncs = make(map[string]Function)
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.functions)
}

func (r *Registry) CountByType(fnType models.FunctionType) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	switch fnType {
	case models.FunctionTypeStart:
		return len(r.startFuncs)
	case models.FunctionTypeNormal:
		return len(r.normalFuncs)
	case models.FunctionTypeEnd:
		return len(r.endFuncs)
	default:
		return 0
	}
}

type FunctionMetadata struct {
	Name    string
	Version string
	Type    models.FunctionType
}

func (r *Registry) ListMetadata() []FunctionMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]FunctionMetadata, 0, len(r.functions))
	for _, fn := range r.functions {
		result = append(result, FunctionMetadata{
			Name:    fn.GetName(),
			Version: fn.GetVersion(),
			Type:    fn.GetType(),
		})
	}
	return result
}
