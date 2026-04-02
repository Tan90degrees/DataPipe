package monitoring

import (
	"sync"
	"time"
)

const (
	MetricsNamespace = "datapipe"
)

type PipelineMetrics struct {
	mu                  sync.RWMutex
	PipelineCount       *CounterVec
	PipelineRunning     *GaugeVec
	PipelineCompleted   *CounterVec
	PipelineFailed      *CounterVec
	PipelineDuration    *HistogramVec
}

func NewPipelineMetrics() *PipelineMetrics {
	m := &PipelineMetrics{}

	m.PipelineCount = NewCounterVec(
		"pipeline_count",
		"Total number of pipelines",
		[]string{"pipeline_id", "status"},
	)

	m.PipelineRunning = NewGaugeVec(
		"pipeline_running",
		"Current number of running pipelines",
		[]string{"pipeline_id"},
	)

	m.PipelineCompleted = NewCounterVec(
		"pipeline_completed",
		"Total number of completed pipelines",
		[]string{"pipeline_id", "status"},
	)

	m.PipelineFailed = NewCounterVec(
		"pipeline_failed",
		"Total number of failed pipelines",
		[]string{"pipeline_id", "error_type"},
	)

	m.PipelineDuration = NewHistogramVec(
		"pipeline_duration_seconds",
		"Pipeline execution duration in seconds",
		[]string{"pipeline_id", "status"},
		[]float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
	)

	return m
}

func (m *PipelineMetrics) IncPipelineCount(pipelineID string, status string) {
	m.PipelineCount.WithLabels(pipelineID, status).Inc()
}

func (m *PipelineMetrics) SetPipelineRunning(pipelineID string, count float64) {
	m.PipelineRunning.WithLabels(pipelineID).Set(count)
}

func (m *PipelineMetrics) IncPipelineCompleted(pipelineID string, status string) {
	m.PipelineCompleted.WithLabels(pipelineID, status).Inc()
}

func (m *PipelineMetrics) IncPipelineFailed(pipelineID string, errorType string) {
	m.PipelineFailed.WithLabels(pipelineID, errorType).Inc()
}

func (m *PipelineMetrics) ObservePipelineDuration(pipelineID string, status string, duration time.Duration) {
	m.PipelineDuration.WithLabels(pipelineID, status).Observe(duration.Seconds())
}

type FunctionMetrics struct {
	mu                 sync.RWMutex
	ExecutionCount     *CounterVec
	SuccessRate        *GaugeVec
	FailureRate        *GaugeVec
	AvgDuration        *GaugeVec
	Throughput         *CounterVec
}

func NewFunctionMetrics() *FunctionMetrics {
	m := &FunctionMetrics{}

	m.ExecutionCount = NewCounterVec(
		"function_execution_count",
		"Total number of function executions",
		[]string{"function_name", "pipeline_id", "node_id"},
	)

	m.SuccessRate = NewGaugeVec(
		"function_success_rate",
		"Function success rate",
		[]string{"function_name", "pipeline_id"},
	)

	m.FailureRate = NewGaugeVec(
		"function_failure_rate",
		"Function failure rate",
		[]string{"function_name", "pipeline_id"},
	)

	m.AvgDuration = NewGaugeVec(
		"function_avg_duration_seconds",
		"Average function execution duration",
		[]string{"function_name", "pipeline_id"},
	)

	m.Throughput = NewCounterVec(
		"function_throughput",
		"Function throughput (items processed)",
		[]string{"function_name", "pipeline_id", "node_id"},
	)

	return m
}

func (m *FunctionMetrics) IncExecutionCount(functionName, pipelineID, nodeID string) {
	m.ExecutionCount.WithLabels(functionName, pipelineID, nodeID).Inc()
}

func (m *FunctionMetrics) SetSuccessRate(functionName, pipelineID string, rate float64) {
	m.SuccessRate.WithLabels(functionName, pipelineID).Set(rate)
}

func (m *FunctionMetrics) SetFailureRate(functionName, pipelineID string, rate float64) {
	m.FailureRate.WithLabels(functionName, pipelineID).Set(rate)
}

func (m *FunctionMetrics) SetAvgDuration(functionName, pipelineID string, duration float64) {
	m.AvgDuration.WithLabels(functionName, pipelineID).Set(duration)
}

func (m *FunctionMetrics) IncThroughput(functionName, pipelineID, nodeID string, count float64) {
	m.Throughput.WithLabels(functionName, pipelineID, nodeID).Add(count)
}

type ResourceMetrics struct {
	mu          sync.RWMutex
	CPUUsage    *GaugeVec
	MemoryUsage *GaugeVec
	DiskUsage   *GaugeVec
	NetworkIO   *CounterVec
}

func NewResourceMetrics() *ResourceMetrics {
	m := &ResourceMetrics{}

	m.CPUUsage = NewGaugeVec(
		"cpu_usage_percent",
		"CPU usage percentage",
		[]string{"host", "instance"},
	)

	m.MemoryUsage = NewGaugeVec(
		"memory_usage_bytes",
		"Memory usage in bytes",
		[]string{"host", "instance"},
	)

	m.DiskUsage = NewGaugeVec(
		"disk_usage_bytes",
		"Disk usage in bytes",
		[]string{"host", "mount_point"},
	)

	m.NetworkIO = NewCounterVec(
		"network_io_bytes",
		"Network I/O in bytes",
		[]string{"host", "interface", "direction"},
	)

	return m
}

func (m *ResourceMetrics) SetCPUUsage(host, instance string, usage float64) {
	m.CPUUsage.WithLabels(host, instance).Set(usage)
}

func (m *ResourceMetrics) SetMemoryUsage(host, instance string, usage float64) {
	m.MemoryUsage.WithLabels(host, instance).Set(usage)
}

func (m *ResourceMetrics) SetDiskUsage(host, mountPoint string, usage float64) {
	m.DiskUsage.WithLabels(host, mountPoint).Set(usage)
}

func (m *ResourceMetrics) IncNetworkIO(host, iface, direction string, bytes float64) {
	m.NetworkIO.WithLabels(host, iface, direction).Add(bytes)
}

type NodeMetrics struct {
	mu             sync.RWMutex
	NodeStatus     *GaugeVec
	TasksRunning   *GaugeVec
}

func NewNodeMetrics() *NodeMetrics {
	m := &NodeMetrics{}

	m.NodeStatus = NewGaugeVec(
		"node_status",
		"Node status (1=active, 0=inactive)",
		[]string{"node_id", "pipeline_id"},
	)

	m.TasksRunning = NewGaugeVec(
		"node_tasks_running",
		"Number of tasks running on node",
		[]string{"node_id", "pipeline_id"},
	)

	return m
}

func (m *NodeMetrics) SetNodeStatus(nodeID, pipelineID string, status float64) {
	m.NodeStatus.WithLabels(nodeID, pipelineID).Set(status)
}

func (m *NodeMetrics) SetTasksRunning(nodeID, pipelineID string, count float64) {
	m.TasksRunning.WithLabels(nodeID, pipelineID).Set(count)
}

type DatabaseMetrics struct {
	mu                sync.RWMutex
	DBConnections     *GaugeVec
	DBQueryDuration   *HistogramVec
	DBQueryTotal      *CounterVec
}

func NewDatabaseMetrics() *DatabaseMetrics {
	m := &DatabaseMetrics{}

	m.DBConnections = NewGaugeVec(
		"db_connections",
		"Number of database connections",
		[]string{"db_name", "state"},
	)

	m.DBQueryDuration = NewHistogramVec(
		"db_query_duration_seconds",
		"Database query duration in seconds",
		[]string{"db_name", "query_type"},
		[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	)

	m.DBQueryTotal = NewCounterVec(
		"db_query_total",
		"Total number of database queries",
		[]string{"db_name", "query_type", "status"},
	)

	return m
}

func (m *DatabaseMetrics) SetConnections(dbName, state string, count float64) {
	m.DBConnections.WithLabels(dbName, state).Set(count)
}

func (m *DatabaseMetrics) ObserveQueryDuration(dbName, queryType string, duration time.Duration) {
	m.DBQueryDuration.WithLabels(dbName, queryType).Observe(duration.Seconds())
}

func (m *DatabaseMetrics) IncQueryTotal(dbName, queryType, status string) {
	m.DBQueryTotal.WithLabels(dbName, queryType, status).Inc()
}

type MetricsRegistry struct {
	mu           sync.RWMutex
	counters     map[string]*CounterVec
	gauges       map[string]*GaugeVec
	histograms   map[string]*HistogramVec
	pipeline     *PipelineMetrics
	function     *FunctionMetrics
	resource     *ResourceMetrics
	node         *NodeMetrics
	database     *DatabaseMetrics
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		counters:   make(map[string]*CounterVec),
		gauges:     make(map[string]*GaugeVec),
		histograms: make(map[string]*HistogramVec),
		pipeline:   NewPipelineMetrics(),
		function:   NewFunctionMetrics(),
		resource:   NewResourceMetrics(),
		node:       NewNodeMetrics(),
		database:   NewDatabaseMetrics(),
	}
}

func (r *MetricsRegistry) Pipeline() *PipelineMetrics {
	return r.pipeline
}

func (r *MetricsRegistry) Function() *FunctionMetrics {
	return r.function
}

func (r *MetricsRegistry) Resource() *ResourceMetrics {
	return r.resource
}

func (r *MetricsRegistry) Node() *NodeMetrics {
	return r.node
}

func (r *MetricsRegistry) Database() *DatabaseMetrics {
	return r.database
}

func (r *MetricsRegistry) RegisterCounter(name string, vec *CounterVec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name] = vec
}

func (r *MetricsRegistry) RegisterGauge(name string, vec *GaugeVec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = vec
}

func (r *MetricsRegistry) RegisterHistogram(name string, vec *HistogramVec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.histograms[name] = vec
}

type CounterVec struct {
	mu       sync.RWMutex
	name     string
	help     string
	counters map[string]*counter
	labels   []string
}

type counter struct {
	value  float64
	labels map[string]string
}

func NewCounterVec(name, help string, labels []string) *CounterVec {
	return &CounterVec{
		name:     name,
		help:     help,
		counters: make(map[string]*counter),
		labels:   labels,
	}
}

func (v *CounterVec) WithLabels(labelValues ...string) *Counter {
	v.mu.Lock()
	defer v.mu.Unlock()

	key := v.makeLabelKey(labelValues)

	if c, ok := v.counters[key]; ok {
		return &Counter{vec: v, key: key, value: &c.value}
	}

	c := &counter{
		labels: v.labelsToMap(labelValues),
	}
	v.counters[key] = c
	return &Counter{vec: v, key: key, value: &c.value}
}

func (v *CounterVec) makeLabelKey(labels []string) string {
	result := ""
	for i, l := range labels {
		if i > 0 {
			result += ","
		}
		result += l
	}
	return result
}

func (v *CounterVec) labelsToMap(labels []string) map[string]string {
	m := make(map[string]string)
	for i, label := range v.labels {
		if i < len(labels) {
			m[label] = labels[i]
		}
	}
	return m
}

type Counter struct {
	vec   *CounterVec
	key   string
	value *float64
}

func (c *Counter) Inc() {
	c.vec.mu.Lock()
	defer c.vec.mu.Unlock()
	if counter, ok := c.vec.counters[c.key]; ok {
		counter.value++
	}
}

func (c *Counter) Add(v float64) {
	c.vec.mu.Lock()
	defer c.vec.mu.Unlock()
	if counter, ok := c.vec.counters[c.key]; ok {
		counter.value += v
	}
}

type GaugeVec struct {
	mu     sync.RWMutex
	name   string
	help   string
	gauges map[string]*gauge
	labels []string
}

type gauge struct {
	value  float64
	labels map[string]string
}

func NewGaugeVec(name, help string, labels []string) *GaugeVec {
	return &GaugeVec{
		name:   name,
		help:   help,
		gauges: make(map[string]*gauge),
		labels: labels,
	}
}

func (v *GaugeVec) WithLabels(labelValues ...string) *Gauge {
	v.mu.Lock()
	defer v.mu.Unlock()

	key := v.makeLabelKey(labelValues)

	if g, ok := v.gauges[key]; ok {
		return &Gauge{vec: v, key: key, value: &g.value}
	}

	g := &gauge{
		labels: v.labelsToMap(labelValues),
	}
	v.gauges[key] = g
	return &Gauge{vec: v, key: key, value: &g.value}
}

func (v *GaugeVec) makeLabelKey(labels []string) string {
	result := ""
	for i, l := range labels {
		if i > 0 {
			result += ","
		}
		result += l
	}
	return result
}

func (v *GaugeVec) labelsToMap(labels []string) map[string]string {
	m := make(map[string]string)
	for i, label := range v.labels {
		if i < len(labels) {
			m[label] = labels[i]
		}
	}
	return m
}

type Gauge struct {
	vec   *GaugeVec
	key   string
	value *float64
}

func (g *Gauge) Set(v float64) {
	g.vec.mu.Lock()
	defer g.vec.mu.Unlock()
	if gauge, ok := g.vec.gauges[g.key]; ok {
		gauge.value = v
	}
}

func (g *Gauge) Inc() {
	g.vec.mu.Lock()
	defer g.vec.mu.Unlock()
	if gauge, ok := g.vec.gauges[g.key]; ok {
		gauge.value++
	}
}

func (g *Gauge) Dec() {
	g.vec.mu.Lock()
	defer g.vec.mu.Unlock()
	if g, ok := g.vec.gauges[g.key]; ok {
		g.value--
	}
}

type HistogramVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	histograms map[string]*histogram
	labels     []string
	buckets    []float64
}

type histogram struct {
	mu           sync.RWMutex
	count        uint64
	sum          float64
	bucketCounts []uint64
	labels       map[string]string
}

func NewHistogramVec(name, help string, labels []string, buckets []float64) *HistogramVec {
	if buckets == nil {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	return &HistogramVec{
		name:       name,
		help:       help,
		histograms: make(map[string]*histogram),
		labels:     labels,
		buckets:    buckets,
	}
}

func (v *HistogramVec) WithLabels(labelValues ...string) *Histogram {
	v.mu.Lock()
	defer v.mu.Unlock()

	key := v.makeLabelKey(labelValues)

	if h, ok := v.histograms[key]; ok {
		return &Histogram{vec: v, key: key, hist: h}
	}

	h := &histogram{
		bucketCounts: make([]uint64, len(v.buckets)),
		labels:        v.labelsToMap(labelValues),
	}
	v.histograms[key] = h
	return &Histogram{vec: v, key: key, hist: h}
}

func (v *HistogramVec) makeLabelKey(labels []string) string {
	result := ""
	for i, l := range labels {
		if i > 0 {
			result += ","
		}
		result += l
	}
	return result
}

func (v *HistogramVec) labelsToMap(labels []string) map[string]string {
	m := make(map[string]string)
	for i, label := range v.labels {
		if i < len(labels) {
			m[label] = labels[i]
		}
	}
	return m
}

type Histogram struct {
	vec  *HistogramVec
	key  string
	hist *histogram
}

func (h *Histogram) Observe(v float64) {
	h.hist.mu.Lock()
	defer h.hist.mu.Unlock()

	h.hist.count++
	h.hist.sum += v

	for i, bucket := range h.vec.buckets {
		if v <= bucket {
			h.hist.bucketCounts[i]++
		}
	}
}

var (
	globalRegistry *MetricsRegistry
	registryOnce   sync.Once
)

func GetRegistry() *MetricsRegistry {
	if globalRegistry == nil {
		registryOnce.Do(func() {
			globalRegistry = NewMetricsRegistry()
		})
	}
	return globalRegistry
}
