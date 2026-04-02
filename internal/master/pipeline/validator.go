package pipeline

import (
	"fmt"

	"datapipe/internal/models"
)

type Validator struct{}

func NewValidator() *Validator {
	return &Validator{}
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func (v *Validator) ValidateDefinition(def *models.Definition) error {
	if def == nil {
		return &ValidationError{Field: "definition", Message: "definition is required"}
	}

	if len(def.Nodes) == 0 {
		return &ValidationError{Field: "nodes", Message: "at least one node is required"}
	}

	if err := v.validateNodes(def.Nodes); err != nil {
		return err
	}

	if err := v.validateEdges(def.Nodes, def.Edges); err != nil {
		return err
	}

	if err := v.validateDAG(def.Nodes, def.Edges); err != nil {
		return err
	}

	if err := v.validateExecutionConfig(&def.Execution); err != nil {
		return err
	}

	return nil
}

func (v *Validator) validateNodes(nodes []models.NodeDefinition) error {
	nodeIDs := make(map[string]bool)

	for _, node := range nodes {
		if node.ID == "" {
			return &ValidationError{Field: "node.id", Message: "node ID is required"}
		}

		if nodeIDs[node.ID] {
			return &ValidationError{Field: "node.id", Message: fmt.Sprintf("duplicate node ID: %s", node.ID)}
		}
		nodeIDs[node.ID] = true

		if node.Type == "" {
			return &ValidationError{Field: "node.type", Message: fmt.Sprintf("node type is required for node %s", node.ID)}
		}

		if node.Function == "" {
			return &ValidationError{Field: "node.function", Message: fmt.Sprintf("node function is required for node %s", node.ID)}
		}
	}

	return nil
}

func (v *Validator) validateEdges(nodes []models.NodeDefinition, edges []models.EdgeDefinition) error {
	nodeIDs := make(map[string]bool)
	for _, node := range nodes {
		nodeIDs[node.ID] = true
	}

	for _, edge := range edges {
		if edge.From == "" {
			return &ValidationError{Field: "edge.from", Message: "edge source is required"}
		}

		if edge.To == "" {
			return &ValidationError{Field: "edge.to", Message: "edge destination is required"}
		}

		if !nodeIDs[edge.From] {
			return &ValidationError{Field: "edge.from", Message: fmt.Sprintf("unknown source node: %s", edge.From)}
		}

		if !nodeIDs[edge.To] {
			return &ValidationError{Field: "edge.to", Message: fmt.Sprintf("unknown destination node: %s", edge.To)}
		}

		if edge.From == edge.To {
			return &ValidationError{Field: "edge", Message: fmt.Sprintf("self-referencing edge not allowed: %s", edge.From)}
		}
	}

	return nil
}

func (v *Validator) validateDAG(nodes []models.NodeDefinition, edges []models.EdgeDefinition) error {
	adjList := make(map[string][]string)
	inDegree := make(map[string]int)
	nodeIDSet := make(map[string]bool)

	for _, node := range nodes {
		adjList[node.ID] = []string{}
		inDegree[node.ID] = 0
		nodeIDSet[node.ID] = true
	}

	for _, edge := range edges {
		adjList[edge.From] = append(adjList[edge.From], edge.To)
		inDegree[edge.To]++
	}

	for _, node := range nodes {
		if node.DependsOn != nil {
			for _, dep := range node.DependsOn {
				if !nodeIDSet[dep] {
					return &ValidationError{Field: "node.depends_on", Message: fmt.Sprintf("unknown dependency node: %s", dep)}
				}
				if !v.hasPath(adjList, dep, node.ID) {
					adjList[dep] = append(adjList[dep], node.ID)
					inDegree[node.ID]++
				}
			}
		}
	}

	var queue []string
	for _, node := range nodes {
		if inDegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}

	if len(queue) == 0 {
		return &ValidationError{Field: "dag", Message: "no source nodes found - pipeline must have at least one node with no dependencies"}
	}

	visitedCount := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visitedCount++

		for _, neighbor := range adjList[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if visitedCount != len(nodes) {
		return &ValidationError{Field: "dag", Message: "cycle detected in pipeline graph - pipeline must be a directed acyclic graph (DAG)"}
	}

	return nil
}

func (v *Validator) hasPath(adjList map[string][]string, from, to string) bool {
	visited := make(map[string]bool)
	return v.dfsPath(adjList, from, to, visited)
}

func (v *Validator) dfsPath(adjList map[string][]string, current, target string, visited map[string]bool) bool {
	if current == target {
		return true
	}

	if visited[current] {
		return false
	}

	visited[current] = true

	for _, neighbor := range adjList[current] {
		if v.dfsPath(adjList, neighbor, target, visited) {
			return true
		}
	}

	return false
}

func (v *Validator) validateExecutionConfig(exec *models.ExecutionConfig) error {
	if exec == nil {
		return nil
	}

	if exec.Parallelism < 0 {
		return &ValidationError{Field: "execution.parallelism", Message: "parallelism must be non-negative"}
	}

	if exec.Parallelism == 0 {
		exec.Parallelism = 1
	}

	if exec.RetryPolicy.MaxRetries < 0 {
		return &ValidationError{Field: "execution.retryPolicy.maxRetries", Message: "maxRetries must be non-negative"}
	}

	return nil
}

func (v *Validator) TopologicalSort(nodes []models.NodeDefinition, edges []models.EdgeDefinition) ([]string, error) {
	adjList := make(map[string][]string)
	inDegree := make(map[string]int)

	for _, node := range nodes {
		adjList[node.ID] = []string{}
		inDegree[node.ID] = 0
	}

	for _, edge := range edges {
		adjList[edge.From] = append(adjList[edge.From], edge.To)
		inDegree[edge.To]++
	}

	for _, node := range nodes {
		if node.DependsOn != nil {
			for _, dep := range node.DependsOn {
				if !v.hasPath(adjList, dep, node.ID) {
					adjList[dep] = append(adjList[dep], node.ID)
					inDegree[node.ID]++
				}
			}
		}
	}

	var queue []string
	for _, node := range nodes {
		if inDegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}

	var result []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		for _, neighbor := range adjList[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(nodes) {
		return nil, &ValidationError{Field: "topological_sort", Message: "cycle detected - cannot compute topological order"}
	}

	return result, nil
}


