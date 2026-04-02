package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"datapipe/internal/models"
)

type MockExecutor struct {
	mu         sync.Mutex
	executions []string
	status     map[string]string
}

func NewMockExecutor() *MockExecutor {
	return &MockExecutor{
		executions: make([]string, 0),
		status:    make(map[string]string),
	}
}

func (e *MockExecutor) Execute(nodeID string, data interface{}) (interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executions = append(e.executions, nodeID)
	e.status[nodeID] = "executed"
	return map[string]interface{}{"result": "processed"}, nil
}

func (e *MockExecutor) GetExecutions() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]string, len(e.executions))
	copy(result, e.executions)
	return result
}

func (e *MockExecutor) GetStatus(nodeID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status[nodeID]
}

type PipelineRunner struct {
	executor *MockExecutor
	nodes    []models.NodeDefinition
	edges    map[string][]string
}

func NewPipelineRunner(def *models.Definition) *PipelineRunner {
	edges := make(map[string][]string)
	for _, edge := range def.Edges {
		edges[edge.From] = append(edges[edge.From], edge.To)
	}

	return &PipelineRunner{
		executor: NewMockExecutor(),
		nodes:    def.Nodes,
		edges:    edges,
	}
}

func (r *PipelineRunner) GetExecutableNodes() []string {
	inDegree := make(map[string]int)
	nodeMap := make(map[string]bool)

	for _, node := range r.nodes {
		nodeMap[node.ID] = true
		if _, exists := inDegree[node.ID]; !exists {
			inDegree[node.ID] = 0
		}
		for _, dep := range node.DependsOn {
			inDegree[node.ID]++
		}
	}

	executable := []string{}
	for _, node := range r.nodes {
		if inDegree[node.ID] == 0 {
			executable = append(executable, node.ID)
		}
	}
	return executable
}

func (r *PipelineRunner) ExecuteNode(nodeID string) error {
	_, err := r.executor.Execute(nodeID, nil)
	return err
}

func hasCycle(nodes []string, edges map[string][]string) bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(string) bool
	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true

		for _, neighbor := range edges[node] {
			if !visited[neighbor] {
				if dfs(neighbor) {
					return true
				}
			} else if recStack[neighbor] {
				return true
			}
		}

		recStack[node] = false
		return false
	}

	for _, node := range nodes {
		if !visited[node] {
			if dfs(node) {
				return true
			}
		}
	}
	return false
}

func TestPipelineLifecycle(t *testing.T) {
	t.Run("create pipeline definition", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "process", Type: "normal", Function: "processFunc", DependsOn: []string{"start"}},
				{ID: "end", Type: "end", Function: "endFunc", DependsOn: []string{"process"}},
			},
			Edges: []models.EdgeDefinition{
				{From: "start", To: "process"},
				{From: "process", To: "end"},
			},
			Execution: models.ExecutionConfig{
				Parallelism: 4,
				RetryPolicy: models.RetryPolicy{
					MaxRetries: 3,
					Backoff:    "exponential",
				},
			},
		}

		if len(def.Nodes) != 3 {
			t.Errorf("expected 3 nodes, got %d", len(def.Nodes))
		}
		if def.Execution.Parallelism != 4 {
			t.Errorf("expected parallelism 4, got %d", def.Execution.Parallelism)
		}
	})

	t.Run("pipeline status transitions", func(t *testing.T) {
		statuses := []models.PipelineStatus{
			models.PipelineStatusDraft,
			models.PipelineStatusActive,
			models.PipelineStatusPaused,
			models.PipelineStatusStopped,
		}

		validTransitions := map[models.PipelineStatus][]models.PipelineStatus{
			models.PipelineStatusDraft:   {models.PipelineStatusActive},
			models.PipelineStatusActive:  {models.PipelineStatusPaused, models.PipelineStatusStopped},
			models.PipelineStatusPaused:  {models.PipelineStatusActive, models.PipelineStatusStopped},
			models.PipelineStatusStopped: {},
		}

		for _, status := range statuses {
			transitions, ok := validTransitions[status]
			if !ok {
				t.Errorf("no transitions defined for status %s", status)
			}
			t.Logf("Status %s can transition to: %v", status, transitions)
		}
	})

	t.Run("pipeline node types", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "process1", Type: "normal", Function: "processFunc1"},
				{ID: "process2", Type: "normal", Function: "processFunc2"},
				{ID: "end", Type: "end", Function: "endFunc"},
			},
		}

		startCount := 0
		endCount := 0
		normalCount := 0

		for _, node := range def.Nodes {
			switch node.Type {
			case models.FunctionTypeStart:
				startCount++
			case models.FunctionTypeEnd:
				endCount++
			case models.FunctionTypeNormal:
				normalCount++
			}
		}

		if startCount != 1 {
			t.Errorf("expected 1 start node, got %d", startCount)
		}
		if endCount != 1 {
			t.Errorf("expected 1 end node, got %d", endCount)
		}
		if normalCount != 2 {
			t.Errorf("expected 2 normal nodes, got %d", normalCount)
		}
	})
}

func TestPipelineExecution(t *testing.T) {
	t.Run("linear pipeline execution", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "step1", Type: "normal", Function: "step1Func", DependsOn: []string{"start"}},
				{ID: "step2", Type: "normal", Function: "step2Func", DependsOn: []string{"step1"}},
				{ID: "end", Type: "end", Function: "endFunc", DependsOn: []string{"step2"}},
			},
		}

		runner := NewPipelineRunner(def)

		execOrder := []string{}
		for _, nodeID := range []string{"start", "step1", "step2", "end"} {
			if err := runner.ExecuteNode(nodeID); err != nil {
				t.Fatalf("failed to execute node %s: %v", nodeID, err)
			}
			execOrder = append(execOrder, nodeID)
		}

		executions := runner.executor.GetExecutions()
		if len(executions) != len(execOrder) {
			t.Errorf("expected %d executions, got %d", len(execOrder), len(executions))
		}
	})

	t.Run("parallel pipeline execution", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "branch1", Type: "normal", Function: "branch1Func", DependsOn: []string{"start"}},
				{ID: "branch2", Type: "normal", Function: "branch2Func", DependsOn: []string{"start"}},
				{ID: "merge", Type: "normal", Function: "mergeFunc", DependsOn: []string{"branch1", "branch2"}},
				{ID: "end", Type: "end", Function: "endFunc", DependsOn: []string{"merge"}},
			},
		}

		runner := NewPipelineRunner(def)

		executable := runner.GetExecutableNodes()
		if len(executable) != 1 || executable[0] != "start" {
			t.Errorf("expected only 'start' to be executable, got %v", executable)
		}

		runner.ExecuteNode("start")

		executable = runner.GetExecutableNodes()
		foundStart := false
		for _, id := range executable {
			if id == "start" {
				foundStart = true
			}
		}
		if foundStart {
			t.Error("'start' should no longer be executable after execution")
		}
	})

	t.Run("pipeline execution with timeout", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "slow", Type: "normal", Function: "slowFunc", DependsOn: []string{"start"}},
				{ID: "end", Type: "end", Function: "endFunc", DependsOn: []string{"slow"}},
			},
		}

		runner := NewPipelineRunner(def)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		done := make(chan error, 1)

		go func() {
			runner.ExecuteNode("start")
			runner.ExecuteNode("slow")
			done <- nil
		}()

		select {
		case <-ctx.Done():
			t.Log("execution timed out as expected")
		case err := <-done:
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
	})

	t.Run("pipeline execution order validation", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "start", Function: "startFunc"},
				{ID: "B", Type: "normal", Function: "bFunc", DependsOn: []string{"A"}},
				{ID: "C", Type: "normal", Function: "cFunc", DependsOn: []string{"A"}},
				{ID: "D", Type: "end", Function: "dFunc", DependsOn: []string{"B", "C"}},
			},
		}

		runner := NewPipelineRunner(def)

		executionOrder := []string{}

		runner.ExecuteNode("A")
		executionOrder = append(executionOrder, "A")

		if err := runner.ExecuteNode("D"); err == nil {
			t.Error("expected error when executing D before its dependencies")
		}

		runner.ExecuteNode("B")
		executionOrder = append(executionOrder, "B")

		if err := runner.ExecuteNode("D"); err == nil {
			t.Error("expected error when executing D before C")
		}

		runner.ExecuteNode("C")
		executionOrder = append(executionOrder, "C")

		if err := runner.ExecuteNode("D"); err != nil {
			t.Errorf("should be able to execute D after B and C: %v", err)
		}
		executionOrder = append(executionOrder, "D")

		t.Logf("Execution order: %v", executionOrder)
	})
}

func TestPipelineDAGValidation(t *testing.T) {
	t.Run("valid DAG", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "start", Function: "startFunc"},
				{ID: "B", Type: "normal", Function: "bFunc", DependsOn: []string{"A"}},
				{ID: "C", Type: "normal", Function: "cFunc", DependsOn: []string{"B"}},
			},
		}

		edges := make(map[string][]string)
		for _, node := range def.Nodes {
			edges[node.ID] = node.DependsOn
		}

		nodes := make([]string, len(def.Nodes))
		for i, node := range def.Nodes {
			nodes[i] = node.ID
		}

		if hasCycle(nodes, edges) {
			t.Error("expected no cycle in valid DAG")
		}
	})

	t.Run("cyclic DAG", func(t *testing.T) {
		def := &models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "start", Function: "startFunc"},
				{ID: "B", Type: "normal", Function: "bFunc", DependsOn: []string{"C"}},
				{ID: "C", Type: "normal", Function: "cFunc", DependsOn: []string{"A"}},
			},
		}

		edges := make(map[string][]string)
		for _, node := range def.Nodes {
			edges[node.ID] = node.DependsOn
		}

		nodes := make([]string, len(def.Nodes))
		for i, node := range def.Nodes {
			nodes[i] = node.ID
		}

		if !hasCycle(nodes, edges) {
			t.Error("expected cycle in cyclic DAG")
		}
	})
}

func TestPipelineVersioning(t *testing.T) {
	t.Run("pipeline version creation", func(t *testing.T) {
		pipeline := &models.Pipeline{
			ID:          "pipeline-1",
			Name:        "test-pipeline",
			Description: "Test pipeline",
			Version:     1,
			Status:      models.PipelineStatusDraft,
		}

		if pipeline.Version != 1 {
			t.Errorf("expected version 1, got %d", pipeline.Version)
		}

		pipeline.Version = 2

		if pipeline.Version != 2 {
			t.Errorf("expected version 2, got %d", pipeline.Version)
		}
	})

	t.Run("pipeline version history", func(t *testing.T) {
		versions := []models.PipelineVersion{
			{
				ID:         "v1",
				PipelineID: "pipeline-1",
				Version:    1,
				Changelog: "initial version",
			},
			{
				ID:         "v2",
				PipelineID: "pipeline-1",
				Version:    2,
				Changelog: "added new node",
			},
			{
				ID:         "v3",
				PipelineID: "pipeline-1",
				Version:    3,
				Changelog: "fixed bug",
			},
		}

		if len(versions) != 3 {
			t.Errorf("expected 3 versions, got %d", len(versions))
		}

		latestVersion := versions[len(versions)-1]
		if latestVersion.Version != 3 {
			t.Errorf("expected latest version 3, got %d", latestVersion.Version)
		}
	})
}
