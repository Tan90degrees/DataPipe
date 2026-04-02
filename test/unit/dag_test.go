package unit

import (
	"testing"

	"datapipe/internal/models"
)

func hasCycle(nodes []string, edges map[string][]string, visited map[string]bool, recStack map[string]bool, node string) bool {
	visited[node] = true
	recStack[node] = true

	for _, neighbor := range edges[node] {
		if !visited[neighbor] {
			if hasCycle(nodes, edges, visited, recStack, neighbor) {
				return true
			}
		} else if recStack[neighbor] {
			return true
		}
	}

	recStack[node] = false
	return false
}

func detectCycle(nodes []string, edges map[string][]string) bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, node := range nodes {
		visited[node] = false
		recStack[node] = false
	}

	for _, node := range nodes {
		if !visited[node] {
			if hasCycle(nodes, edges, visited, recStack, node) {
				return true
			}
		}
	}
	return false
}

func topologicalSort(nodes []string, edges map[string][]string) ([]string, bool) {
	inDegree := make(map[string]int)
	for _, node := range nodes {
		inDegree[node] = 0
	}
	for _, deps := range edges {
		for _, dep := range deps {
			inDegree[dep]++
		}
	}

	queue := []string{}
	for _, node := range nodes {
		if inDegree[node] == 0 {
			queue = append(queue, node)
		}
	}

	result := []string{}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		for _, neighbor := range edges[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	return result, len(result) == len(nodes)
}

func TestDAGValidation(t *testing.T) {
	t.Run("no cycle", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		edges := map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {},
		}

		if detectCycle(nodes, edges) {
			t.Error("expected no cycle, but detected one")
		}
	})

	t.Run("simple cycle", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		edges := map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		}

		if !detectCycle(nodes, edges) {
			t.Error("expected cycle, but detected none")
		}
	})

	t.Run("self loop", func(t *testing.T) {
		nodes := []string{"A"}
		edges := map[string][]string{
			"A": {"A"},
		}

		if !detectCycle(nodes, edges) {
			t.Error("expected self loop cycle, but detected none")
		}
	})

	t.Run("complex diamond cycle", func(t *testing.T) {
		nodes := []string{"A", "B", "C", "D", "E"}
		edges := map[string][]string{
			"A": {"B", "C"},
			"B": {"D"},
			"C": {"D"},
			"D": {"E"},
			"E": {"B"},
		}

		if !detectCycle(nodes, edges) {
			t.Error("expected cycle in complex graph, but detected none")
		}
	})

	t.Run("disconnected components no cycle", func(t *testing.T) {
		nodes := []string{"A", "B", "C", "D"}
		edges := map[string][]string{
			"A": {"B"},
			"B": {},
			"C": {"D"},
			"D": {},
		}

		if detectCycle(nodes, edges) {
			t.Error("expected no cycle in disconnected graph")
		}
	})
}

func TestTopologicalSort(t *testing.T) {
	t.Run("simple linear", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		edges := map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {},
		}

		result, valid := topologicalSort(nodes, edges)
		if !valid {
			t.Error("expected valid topological order")
		}
		if len(result) != len(nodes) {
			t.Errorf("expected %d nodes in result, got %d", len(nodes), len(result))
		}
	})

	t.Run("complex graph", func(t *testing.T) {
		nodes := []string{"A", "B", "C", "D"}
		edges := map[string][]string{
			"A": {"B", "C"},
			"B": {"D"},
			"C": {"D"},
			"D": {},
		}

		result, valid := topologicalSort(nodes, edges)
		if !valid {
			t.Error("expected valid topological order")
		}

		aIdx := -1
		bIdx := -1
		cIdx := -1
		dIdx := -1
		for i, node := range result {
			switch node {
			case "A":
				aIdx = i
			case "B":
				bIdx = i
			case "C":
				cIdx = i
			case "D":
				dIdx = i
			}
		}

		if aIdx > bIdx {
			t.Error("A should come before B in topological order")
		}
		if aIdx > cIdx {
			t.Error("A should come before C in topological order")
		}
		if bIdx > dIdx {
			t.Error("B should come before D in topological order")
		}
		if cIdx > dIdx {
			t.Error("C should come before D in topological order")
		}
	})

	t.Run("cycle detection", func(t *testing.T) {
		nodes := []string{"A", "B", "C"}
		edges := map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		}

		_, valid := topologicalSort(nodes, edges)
		if valid {
			t.Error("expected invalid topological order for cyclic graph")
		}
	})

	t.Run("parallel branches", func(t *testing.T) {
		nodes := []string{"Start", "A", "B", "C", "End"}
		edges := map[string][]string{
			"Start": {"A", "B"},
			"A":     {"C"},
			"B":     {"C"},
			"C":     {"End"},
			"End":   {},
		}

		result, valid := topologicalSort(nodes, edges)
		if !valid {
			t.Error("expected valid topological order")
		}

		startIdx := -1
		endIdx := -1
		for i, node := range result {
			if node == "Start" {
				startIdx = i
			}
			if node == "End" {
				endIdx = i
			}
		}

		if startIdx > endIdx {
			t.Error("Start should come before End")
		}
	})
}

func TestPipelineDefinition(t *testing.T) {
	t.Run("valid definition", func(t *testing.T) {
		def := models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "start", Type: "start", Function: "startFunc"},
				{ID: "process1", Type: "normal", Function: "processFunc1", DependsOn: []string{"start"}},
				{ID: "process2", Type: "normal", Function: "processFunc2", DependsOn: []string{"start"}},
				{ID: "merge", Type: "normal", Function: "mergeFunc", DependsOn: []string{"process1", "process2"}},
				{ID: "end", Type: "end", Function: "endFunc", DependsOn: []string{"merge"}},
			},
			Edges: []models.EdgeDefinition{
				{From: "start", To: "process1"},
				{From: "start", To: "process2"},
				{From: "process1", To: "merge"},
				{From: "process2", To: "merge"},
				{From: "merge", To: "end"},
			},
			Execution: models.ExecutionConfig{
				Parallelism: 4,
				RetryPolicy: models.RetryPolicy{
					MaxRetries: 3,
					Backoff:    "exponential",
				},
			},
		}

		nodes := make([]string, len(def.Nodes))
		edges := make(map[string][]string)
		for i, node := range def.Nodes {
			nodes[i] = node.ID
			edges[node.ID] = node.DependsOn
		}

		if detectCycle(nodes, edges) {
			t.Error("expected no cycle in valid pipeline definition")
		}

		result, valid := topologicalSort(nodes, edges)
		if !valid {
			t.Error("expected valid topological order for pipeline")
		}

		startIdx := -1
		endIdx := -1
		for i, node := range result {
			if node == "start" {
				startIdx = i
			}
			if node == "end" {
				endIdx = i
			}
		}

		if startIdx > endIdx {
			t.Error("start node should come before end node")
		}
	})

	t.Run("cycle in definition", func(t *testing.T) {
		def := models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "normal", Function: "funcA"},
				{ID: "B", Type: "normal", Function: "funcB", DependsOn: []string{"C"}},
				{ID: "C", Type: "normal", Function: "funcC", DependsOn: []string{"A"}},
			},
			Edges: []models.EdgeDefinition{
				{From: "A", To: "B"},
				{From: "B", To: "C"},
				{From: "C", To: "A"},
			},
		}

		nodes := make([]string, len(def.Nodes))
		edges := make(map[string][]string)
		for i, node := range def.Nodes {
			nodes[i] = node.ID
			edges[node.ID] = node.DependsOn
		}

		if !detectCycle(nodes, edges) {
			t.Error("expected cycle in cyclic pipeline definition")
		}
	})

	t.Run("missing dependency", func(t *testing.T) {
		def := models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "normal", Function: "funcA"},
				{ID: "B", Type: "normal", Function: "funcB", DependsOn: []string{"nonexistent"}},
			},
		}

		nodeIDs := make(map[string]bool)
		for _, node := range def.Nodes {
			nodeIDs[node.ID] = true
		}

		for _, node := range def.Nodes {
			for _, dep := range node.DependsOn {
				if !nodeIDs[dep] {
					t.Errorf("node %s depends on non-existent node %s", node.ID, dep)
				}
			}
		}
	})

	t.Run("node config", func(t *testing.T) {
		def := models.Definition{
			Nodes: []models.NodeDefinition{
				{
					ID:       "process",
					Type:     "normal",
					Function: "processFunc",
					Config: map[string]interface{}{
						"batch_size": 100,
						"timeout":    30,
						"retries":    3,
					},
				},
			},
		}

		if len(def.Nodes) != 1 {
			t.Errorf("expected 1 node, got %d", len(def.Nodes))
		}

		node := def.Nodes[0]
		if node.Config["batch_size"].(int) != 100 {
			t.Errorf("expected batch_size=100, got %v", node.Config["batch_size"])
		}
	})

	t.Run("execution config", func(t *testing.T) {
		def := models.Definition{
			Nodes: []models.NodeDefinition{
				{ID: "A", Type: "start", Function: "startFunc"},
				{ID: "B", Type: "end", Function: "endFunc", DependsOn: []string{"A"}},
			},
			Execution: models.ExecutionConfig{
				Parallelism: 10,
				RetryPolicy: models.RetryPolicy{
					MaxRetries: 5,
					Backoff:    "linear",
				},
			},
		}

		if def.Execution.Parallelism != 10 {
			t.Errorf("expected parallelism=10, got %d", def.Execution.Parallelism)
		}
		if def.Execution.RetryPolicy.MaxRetries != 5 {
			t.Errorf("expected max_retries=5, got %d", def.Execution.RetryPolicy.MaxRetries)
		}
		if def.Execution.RetryPolicy.Backoff != "linear" {
			t.Errorf("expected backoff='linear', got '%s'", def.Execution.RetryPolicy.Backoff)
		}
	})
}

func TestPipelineDefinitionScan(t *testing.T) {
	jsonData := `{"nodes":[{"id":"start","type":"start","function":"startFunc"},{"id":"end","type":"end","function":"endFunc","depends_on":["start"]}],"edges":[{"from":"start","to":"end"}],"execution":{"parallelism":1,"retryPolicy":{"maxRetries":3,"backoff":"exponential"}}}`

	var def models.Definition
	if err := def.Scan([]byte(jsonData)); err != nil {
		t.Fatalf("failed to scan definition: %v", err)
	}

	if len(def.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(def.Nodes))
	}

	if def.Nodes[0].ID != "start" {
		t.Errorf("expected first node id='start', got '%s'", def.Nodes[0].ID)
	}

	if def.Nodes[1].DependsOn[0] != "start" {
		t.Errorf("expected end node depends on 'start', got '%v'", def.Nodes[1].DependsOn)
	}
}

func TestPipelineDefinitionValue(t *testing.T) {
	def := models.Definition{
		Nodes: []models.NodeDefinition{
			{ID: "A", Type: "start", Function: "startFunc"},
			{ID: "B", Type: "end", Function: "endFunc", DependsOn: []string{"A"}},
		},
		Execution: models.ExecutionConfig{
			Parallelism: 2,
			RetryPolicy: models.RetryPolicy{
				MaxRetries: 3,
				Backoff:    "exponential",
			},
		},
	}

	value, err := def.Value()
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}

	valueBytes, ok := value.([]byte)
	if !ok {
		t.Fatal("expected []byte value")
	}

	var scannedDef models.Definition
	if err := scannedDef.Scan(valueBytes); err != nil {
		t.Fatalf("failed to scan value: %v", err)
	}

	if len(scannedDef.Nodes) != len(def.Nodes) {
		t.Errorf("expected %d nodes, got %d", len(def.Nodes), len(scannedDef.Nodes))
	}
}
