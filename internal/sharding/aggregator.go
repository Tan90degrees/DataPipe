package sharding

import (
	"context"
	"sort"
	"sync"

	"datapipe/internal/common/errors"
)

type AggregationType string

const (
	AggregationTypeMerge  AggregationType = "merge"
	AggregationTypeConcat AggregationType = "concat"
	AggregationTypeReduce AggregationType = "reduce"
)

type Aggregator interface {
	Aggregate(ctx context.Context, results []*Task) (*AggregationResult, error)
	Type() AggregationType
}

type AggregationResult struct {
	Data        interface{}
	TotalCount  int
	SuccessCount int
	FailedCount int
	Errors      []*errors.Error
	Metadata    map[string]interface{}
}

func NewAggregationResult() *AggregationResult {
	return &AggregationResult{
		Errors:   make([]*errors.Error, 0),
		Metadata: make(map[string]interface{}),
	}
}

type MergeAggregator struct {
	mu sync.Mutex
}

func NewMergeAggregator() *MergeAggregator {
	return &MergeAggregator{}
}

func (m *MergeAggregator) Aggregate(ctx context.Context, results []*Task) (*AggregationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := NewAggregationResult()
	result.TotalCount = len(results)

	merged := make(map[string]interface{})

	for _, task := range results {
		if task.Status == TaskStatusCompleted {
			result.SuccessCount++
			if task.Result != nil {
				if data, ok := task.Result.(map[string]interface{}); ok {
					for k, v := range data {
						merged[k] = v
					}
				}
			}
		} else if task.Status == TaskStatusFailed {
			result.FailedCount++
			if task.Error != nil {
				result.Errors = append(result.Errors, task.Error)
			}
		}
	}

	result.Data = merged
	return result, nil
}

func (m *MergeAggregator) Type() AggregationType {
	return AggregationTypeMerge
}

type ConcatAggregator struct {
	mu         sync.Mutex
	preserveOrder bool
}

func NewConcatAggregator(preserveOrder bool) *ConcatAggregator {
	return &ConcatAggregator{
		preserveOrder: preserveOrder,
	}
}

func (c *ConcatAggregator) Aggregate(ctx context.Context, results []*Task) (*AggregationResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := NewAggregationResult()
	result.TotalCount = len(results)

	var items []interface{}

	for _, task := range results {
		if task.Status == TaskStatusCompleted {
			result.SuccessCount++
			if task.Result != nil {
				items = append(items, task.Result)
			}
		} else if task.Status == TaskStatusFailed {
			result.FailedCount++
			if task.Error != nil {
				result.Errors = append(result.Errors, task.Error)
			}
		}
	}

	if c.preserveOrder {
		sort.Slice(items, func(i, j int) bool {
			t1, ok1 := items[i].(*Task)
			t2, ok2 := items[j].(*Task)
			if ok1 && ok2 {
				return t1.Shard.Index < t2.Shard.Index
			}
			return false
		})
	}

	result.Data = items
	return result, nil
}

func (c *ConcatAggregator) Type() AggregationType {
	return AggregationTypeConcat
}

type ReduceAggregator struct {
	mu          sync.Mutex
	reduceFunc  func(results []interface{}) (interface{}, error)
}

func NewReduceAggregator(reduceFunc func(results []interface{}) (interface{}, error)) *ReduceAggregator {
	return &ReduceAggregator{
		reduceFunc: reduceFunc,
	}
}

func (r *ReduceAggregator) Aggregate(ctx context.Context, results []*Task) (*AggregationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := NewAggregationResult()
	result.TotalCount = len(results)

	successResults := make([]interface{}, 0, len(results))

	for _, task := range results {
		if task.Status == TaskStatusCompleted {
			result.SuccessCount++
			if task.Result != nil {
				successResults = append(successResults, task.Result)
			}
		} else if task.Status == TaskStatusFailed {
			result.FailedCount++
			if task.Error != nil {
				result.Errors = append(result.Errors, task.Error)
			}
		}
	}

	if len(successResults) > 0 && r.reduceFunc != nil {
		reduced, err := r.reduceFunc(successResults)
		if err != nil {
			return nil, errors.Wrap(err, errors.ErrCodeInternalServerError, "reduce operation failed")
		}
		result.Data = reduced
	}

	return result, nil
}

func (r *ReduceAggregator) Type() AggregationType {
	return AggregationTypeReduce
}

type OrderedAggregator struct {
	mu          sync.Mutex
	keyFunc     func(interface{}) string
	shardIDFunc func(interface{}) ShardID
}

func NewOrderedAggregator(keyFunc func(interface{}) string, shardIDFunc func(interface{}) ShardID) *OrderedAggregator {
	return &OrderedAggregator{
		keyFunc:     keyFunc,
		shardIDFunc: shardIDFunc,
	}
}

func (o *OrderedAggregator) Aggregate(ctx context.Context, results []*Task) (*AggregationResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := NewAggregationResult()
	result.TotalCount = len(results)

	sortedResults := make([]*Task, len(results))
	copy(sortedResults, results)

	sort.Slice(sortedResults, func(i, j int) bool {
		if sortedResults[i].Shard == nil || sortedResults[j].Shard == nil {
			return false
		}
		return sortedResults[i].Shard.Index < sortedResults[j].Shard.Index
	})

	orderedData := make([]interface{}, 0, len(sortedResults))

	for _, task := range sortedResults {
		if task.Status == TaskStatusCompleted {
			result.SuccessCount++
			if task.Result != nil {
				orderedData = append(orderedData, task.Result)
			}
		} else if task.Status == TaskStatusFailed {
			result.FailedCount++
			if task.Error != nil {
				result.Errors = append(result.Errors, task.Error)
			}
		}
	}

	result.Data = orderedData
	return result, nil
}

func (o *OrderedAggregator) Type() AggregationType {
	return AggregationTypeMerge
}

type AggregatorConfig struct {
	Type          AggregationType `json:"type"`
	PreserveOrder bool            `json:"preserve_order,omitempty"`
}

func NewAggregatorFromConfig(config AggregatorConfig, reduceFunc func(results []interface{}) (interface{}, error)) Aggregator {
	switch config.Type {
	case AggregationTypeMerge:
		return NewMergeAggregator()
	case AggregationTypeConcat:
		return NewConcatAggregator(config.PreserveOrder)
	case AggregationTypeReduce:
		return NewReduceAggregator(reduceFunc)
	default:
		return NewMergeAggregator()
	}
}

func AggregateResults(ctx context.Context, results []*Task, aggType AggregationType) (*AggregationResult, error) {
	var aggregator Aggregator

	switch aggType {
	case AggregationTypeMerge:
		aggregator = NewMergeAggregator()
	case AggregationTypeConcat:
		aggregator = NewConcatAggregator(true)
	case AggregationTypeReduce:
		aggregator = NewReduceAggregator(func(results []interface{}) (interface{}, error) {
			return results, nil
		})
	default:
		aggregator = NewMergeAggregator()
	}

	return aggregator.Aggregate(ctx, results)
}

func FilterSuccessfulResults(results []*Task) []*Task {
	successful := make([]*Task, 0, len(results))
	for _, task := range results {
		if task.Status == TaskStatusCompleted {
			successful = append(successful, task)
		}
	}
	return successful
}

func FilterFailedResults(results []*Task) []*Task {
	failed := make([]*Task, 0, len(results))
	for _, task := range results {
		if task.Status == TaskStatusFailed {
			failed = append(failed, task)
		}
	}
	return failed
}

func CollectErrors(results []*Task) []*errors.Error {
	errs := make([]*errors.Error, 0)
	for _, task := range results {
		if task.Status == TaskStatusFailed && task.Error != nil {
			errs = append(errs, task.Error)
		}
	}
	return errs
}
