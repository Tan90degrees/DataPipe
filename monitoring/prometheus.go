package monitoring

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

func init() {
	GetRegistry()
}

type PrometheusServer struct {
	mu      sync.RWMutex
	addr    string
	path    string
	server  *http.Server
	handler *PrometheusHandler
}

type PrometheusHandler struct {
	registry *MetricsRegistry
}

func NewPrometheusServer(addr, path string) *PrometheusServer {
	if path == "" {
		path = "/metrics"
	}
	if addr == "" {
		addr = ":9090"
	}

	return &PrometheusServer{
		addr: addr,
		path: path,
		handler: &PrometheusHandler{
			registry: GetRegistry(),
		},
	}
}

func (s *PrometheusServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.handler.ServeHTTP)
	mux.HandleFunc("/health", s.healthHandler)

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s.server.ListenAndServe()
}

func (s *PrometheusServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func (s *PrometheusServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *PrometheusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	registry := h.registry

	var output string

	registry.pipeline.PipelineCount.mu.RLock()
	for key, c := range registry.pipeline.PipelineCount.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.pipeline.PipelineCount.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.pipeline.PipelineCount.mu.RUnlock()

	registry.pipeline.PipelineRunning.mu.RLock()
	for key, g := range registry.pipeline.PipelineRunning.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.pipeline.PipelineRunning.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.pipeline.PipelineRunning.mu.RUnlock()

	registry.pipeline.PipelineCompleted.mu.RLock()
	for key, c := range registry.pipeline.PipelineCompleted.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.pipeline.PipelineCompleted.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.pipeline.PipelineCompleted.mu.RUnlock()

	registry.pipeline.PipelineFailed.mu.RLock()
	for key, c := range registry.pipeline.PipelineFailed.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.pipeline.PipelineFailed.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.pipeline.PipelineFailed.mu.RUnlock()

	registry.pipeline.PipelineDuration.mu.RLock()
	for key, h := range registry.pipeline.PipelineDuration.histograms {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.pipeline.PipelineDuration.help)
		output += fmt.Sprintf("# TYPE %s histogram\n", key)
		output += fmt.Sprintf("%s_sum {%s} %f\n", key, key, h.sum)
		output += fmt.Sprintf("%s_count {%s} %d\n", key, key, h.count)
		for i, bucket := range registry.pipeline.PipelineDuration.buckets {
			output += fmt.Sprintf("%s_bucket {%s,le=\"%f\"} %d\n", key, key, bucket, h.bucketCounts[i])
		}
	}
	registry.pipeline.PipelineDuration.mu.RUnlock()

	registry.function.ExecutionCount.mu.RLock()
	for key, c := range registry.function.ExecutionCount.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.function.ExecutionCount.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.function.ExecutionCount.mu.RUnlock()

	registry.function.SuccessRate.mu.RLock()
	for key, g := range registry.function.SuccessRate.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.function.SuccessRate.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.function.SuccessRate.mu.RUnlock()

	registry.function.FailureRate.mu.RLock()
	for key, g := range registry.function.FailureRate.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.function.FailureRate.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.function.FailureRate.mu.RUnlock()

	registry.function.AvgDuration.mu.RLock()
	for key, g := range registry.function.AvgDuration.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.function.AvgDuration.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.function.AvgDuration.mu.RUnlock()

	registry.function.Throughput.mu.RLock()
	for key, c := range registry.function.Throughput.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.function.Throughput.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.function.Throughput.mu.RUnlock()

	registry.resource.CPUUsage.mu.RLock()
	for key, g := range registry.resource.CPUUsage.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.resource.CPUUsage.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.resource.CPUUsage.mu.RUnlock()

	registry.resource.MemoryUsage.mu.RLock()
	for key, g := range registry.resource.MemoryUsage.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.resource.MemoryUsage.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.resource.MemoryUsage.mu.RUnlock()

	registry.resource.DiskUsage.mu.RLock()
	for key, g := range registry.resource.DiskUsage.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.resource.DiskUsage.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.resource.DiskUsage.mu.RUnlock()

	registry.resource.NetworkIO.mu.RLock()
	for key, c := range registry.resource.NetworkIO.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.resource.NetworkIO.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.resource.NetworkIO.mu.RUnlock()

	registry.node.NodeStatus.mu.RLock()
	for key, g := range registry.node.NodeStatus.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.node.NodeStatus.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.node.NodeStatus.mu.RUnlock()

	registry.node.TasksRunning.mu.RLock()
	for key, g := range registry.node.TasksRunning.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.node.TasksRunning.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.node.TasksRunning.mu.RUnlock()

	registry.database.DBConnections.mu.RLock()
	for key, g := range registry.database.DBConnections.gauges {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.database.DBConnections.help)
		output += fmt.Sprintf("# TYPE %s gauge\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, g.value)
	}
	registry.database.DBConnections.mu.RUnlock()

	registry.database.DBQueryDuration.mu.RLock()
	for key, h := range registry.database.DBQueryDuration.histograms {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.database.DBQueryDuration.help)
		output += fmt.Sprintf("# TYPE %s histogram\n", key)
		output += fmt.Sprintf("%s_sum {%s} %f\n", key, key, h.sum)
		output += fmt.Sprintf("%s_count {%s} %d\n", key, key, h.count)
		for i, bucket := range registry.database.DBQueryDuration.buckets {
			output += fmt.Sprintf("%s_bucket {%s,le=\"%f\"} %d\n", key, key, bucket, h.bucketCounts[i])
		}
	}
	registry.database.DBQueryDuration.mu.RUnlock()

	registry.database.DBQueryTotal.mu.RLock()
	for key, c := range registry.database.DBQueryTotal.counters {
		output += fmt.Sprintf("# HELP %s %s\n", key, registry.database.DBQueryTotal.help)
		output += fmt.Sprintf("# TYPE %s counter\n", key)
		output += fmt.Sprintf("%s {%s} %f\n", key, key, c.value)
	}
	registry.database.DBQueryTotal.mu.RUnlock()

	w.Write([]byte(output))
}

func Handler() http.Handler {
	return &PrometheusHandler{
		registry: GetRegistry(),
	}
}

type MetricsExporter struct {
	registry *MetricsRegistry
}

func NewMetricsExporter() *MetricsExporter {
	return &MetricsExporter{
		registry: GetRegistry(),
	}
}

func (e *MetricsExporter) ExportText() (string, error) {
	var output string

	output += "# HELP datapipe_pipeline_count Total number of pipelines\n"
	output += "# TYPE datapipe_pipeline_count counter\n"

	output += "# HELP datapipe_pipeline_running Current number of running pipelines\n"
	output += "# TYPE datapipe_pipeline_running gauge\n"

	output += "# HELP datapipe_pipeline_completed Total number of completed pipelines\n"
	output += "# TYPE datapipe_pipeline_completed counter\n"

	output += "# HELP datapipe_pipeline_failed Total number of failed pipelines\n"
	output += "# TYPE datapipe_pipeline_failed counter\n"

	output += "# HELP datapipe_pipeline_duration_seconds Pipeline execution duration in seconds\n"
	output += "# TYPE datapipe_pipeline_duration_seconds histogram\n"

	output += "# HELP datapipe_function_execution_count Total number of function executions\n"
	output += "# TYPE datapipe_function_execution_count counter\n"

	output += "# HELP datapipe_function_success_rate Function success rate\n"
	output += "# TYPE datapipe_function_success_rate gauge\n"

	output += "# HELP datapipe_function_failure_rate Function failure rate\n"
	output += "# TYPE datapipe_function_failure_rate gauge\n"

	output += "# HELP datapipe_function_avg_duration_seconds Average function execution duration\n"
	output += "# TYPE datapipe_function_avg_duration_seconds gauge\n"

	output += "# HELP datapipe_function_throughput Function throughput\n"
	output += "# TYPE datapipe_function_throughput counter\n"

	output += "# HELP datapipe_cpu_usage_percent CPU usage percentage\n"
	output += "# TYPE datapipe_cpu_usage_percent gauge\n"

	output += "# HELP datapipe_memory_usage_bytes Memory usage in bytes\n"
	output += "# TYPE datapipe_memory_usage_bytes gauge\n"

	output += "# HELP datapipe_disk_usage_bytes Disk usage in bytes\n"
	output += "# TYPE datapipe_disk_usage_bytes gauge\n"

	output += "# HELP datapipe_network_io_bytes Network I/O in bytes\n"
	output += "# TYPE datapipe_network_io_bytes counter\n"

	output += "# HELP datapipe_node_status Node status\n"
	output += "# TYPE datapipe_node_status gauge\n"

	output += "# HELP datapipe_node_tasks_running Number of tasks running on node\n"
	output += "# TYPE datapipe_node_tasks_running gauge\n"

	output += "# HELP datapipe_db_connections Number of database connections\n"
	output += "# TYPE datapipe_db_connections gauge\n"

	output += "# HELP datapipe_db_query_duration_seconds Database query duration in seconds\n"
	output += "# TYPE datapipe_db_query_duration_seconds histogram\n"

	output += "# HELP datapipe_db_query_total Total number of database queries\n"
	output += "# TYPE datapipe_db_query_total counter\n"

	return output, nil
}
