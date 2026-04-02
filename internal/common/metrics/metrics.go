package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MetricsNamespace = "datapipe"

	LabelService    = "service"
	LabelPipeline   = "pipeline"
	LabelNode       = "node"
	LabelTask       = "task"
	LabelStatus     = "status"
	LabelWorker     = "worker"
	LabelType       = "type"
	LabelOperation  = "operation"
)

const (
	TaskStartedTotal          = "task_started_total"
	TaskCompletedTotal        = "task_completed_total"
	TaskFailedTotal           = "task_failed_total"
	TaskRetryTotal            = "task_retry_total"
	TaskDurationSeconds       = "task_duration_seconds"
	TaskQueueSize             = "task_queue_size"
	TaskProcessingSize        = "task_processing_size"

	PipelineCreatedTotal      = "pipeline_created_total"
	PipelineRunningTotal      = "pipeline_running_total"
	PipelineCompletedTotal    = "pipeline_completed_total"
	PipelineFailedTotal        = "pipeline_failed_total"
	PipelineDurationSeconds   = "pipeline_duration_seconds"

	NodeProcessedTotal        = "node_processed_total"
	NodeFailedTotal           = "node_failed_total"
	NodeDurationSeconds       = "node_duration_seconds"
	NodeQueueDepth            = "node_queue_depth"
	NodeThroughputBytes       = "node_throughput_bytes"

	WorkerRegisteredTotal     = "worker_registered_total"
	WorkerHeartbeatTotal      = "worker_heartbeat_total"
	WorkerLostTotal           = "worker_lost_total"
	WorkerTaskCapacity        = "worker_task_capacity"
	WorkerActiveTasks         = "worker_active_tasks"

	DatabaseQueryTotal         = "database_query_total"
	DatabaseQueryDuration      = "database_query_duration_seconds"
	DatabaseConnectionPool     = "database_connection_pool"
	DatabaseConnectionInUse    = "database_connection_in_use"

	RedisOperationTotal        = "redis_operation_total"
	RedisOperationDuration     = "redis_operation_duration_seconds"
	RedisConnectionPool        = "redis_connection_pool"

	HTTPRequestTotal          = "http_request_total"
	HTTPRequestDuration        = "http_request_duration_seconds"
	HTTPUseppgradeTotal        = "http_upgrade_total"

	GrpcRequestTotal           = "grpc_request_total"
	GrpcRequestDuration        = "grpc_request_duration_seconds"

	QueueSize                  = "queue_size"
	QueueCapacity              = "queue_capacity"

	BufferSize                 = "buffer_size"
	BufferCapacity             = "buffer_capacity"

	BytesReadTotal             = "bytes_read_total"
	BytesWrittenTotal          = "bytes_written_total"

	ErrorsTotal                = "errors_total"

	CacheHitTotal              = "cache_hit_total"
	CacheMissTotal             = "cache_miss_total"

	RateLimitReachedTotal      = "rate_limit_reached_total"
	BackoffTotal               = "backoff_total"
)

type Counter interface {
	Inc(args ...float64)
	Add(args ...float64)
	Get() float64
	WithLabels(labels ...string) Counter
	Reset()
}

type Gauge interface {
	Set(value float64)
	Inc(args ...float64)
	Dec(args ...float64)
	Get() float64
	WithLabels(labels ...string) Gauge
}

type Histogram interface {
	Observe(value float64)
	WithLabels(labels ...string) Histogram
}

type Summary interface {
	Observe(value float64)
	WithLabels(labels ...string) Summary
}

type metricsCounter struct {
	mu     sync.RWMutex
	value  float64
	labels map[string]string
	vec    *CounterVec
}

func (c *metricsCounter) Inc(args ...float64) {
	c.Add(args...)
}

func (c *metricsCounter) Add(args ...float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(args) > 0 {
		c.value += args[0]
	} else {
		c.value += 1
	}
}

func (c *metricsCounter) Get() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

func (c *metricsCounter) WithLabels(labels ...string) Counter {
	if c.vec == nil {
		return c
	}
	key := c.vec.makeLabelKey(labels)
	c.vec.mu.RLock()
	defer c.vec.mu.RUnlock()
	if counter, ok := c.vec.counters[key]; ok {
		return counter
	}
	return c
}

func (c *metricsCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = 0
}

type metricsGauge struct {
	mu     sync.RWMutex
	value  float64
	labels map[string]string
	vec    *GaugeVec
}

func (g *metricsGauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

func (g *metricsGauge) Inc(args ...float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(args) > 0 {
		g.value += args[0]
	} else {
		g.value += 1
	}
}

func (g *metricsGauge) Dec(args ...float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(args) > 0 {
		g.value -= args[0]
	} else {
		g.value -= 1
	}
}

func (g *metricsGauge) Get() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

func (g *metricsGauge) WithLabels(labels ...string) Gauge {
	if g.vec == nil {
		return g
	}
	key := g.vec.makeLabelKey(labels)
	g.vec.mu.RLock()
	defer g.vec.mu.RUnlock()
	if gauge, ok := g.vec.gauges[key]; ok {
		return gauge
	}
	return g
}

type metricsHistogram struct {
	mu           sync.RWMutex
	buckets      []float64
	bucketCounts []uint64
	count        uint64
	sum          float64
	labels       map[string]string
	vec          *HistogramVec
}

func (h *metricsHistogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += value
	for i, bucket := range h.buckets {
		if value <= bucket {
			h.bucketCounts[i]++
		}
	}
}

func (h *metricsHistogram) WithLabels(labels ...string) Histogram {
	if h.vec == nil {
		return h
	}
	key := h.vec.makeLabelKey(labels)
	h.vec.mu.RLock()
	defer h.vec.mu.RUnlock()
	if hist, ok := h.vec.histograms[key]; ok {
		return hist
	}
	return h
}

type CounterVec struct {
	mu       sync.RWMutex
	name     string
	help     string
	counters map[string]*metricsCounter
	labels   []string
}

func NewCounterVec(name, help string, labels []string) *CounterVec {
	return &CounterVec{
		name:     name,
		help:     help,
		counters: make(map[string]*metricsCounter),
		labels:   labels,
	}
}

func (v *CounterVec) WithLabels(labels ...string) Counter {
	key := v.makeLabelKey(labels)

	v.mu.Lock()
	defer v.mu.Unlock()

	if counter, ok := v.counters[key]; ok {
		return counter
	}

	counter := &metricsCounter{
		vec:    v,
		labels: v.labelsToMap(labels),
	}
	v.counters[key] = counter
	return counter
}

func (v *CounterVec) makeLabelKey(labels []string) string {
	return strings.Join(labels, ",")
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

func (v *CounterVec) GetAll() map[string]float64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := make(map[string]float64)
	for key, counter := range v.counters {
		result[fmt.Sprintf("%s{%s}", v.name, key)] = counter.Get()
	}
	return result
}

type GaugeVec struct {
	mu     sync.RWMutex
	name   string
	help   string
	gauges map[string]*metricsGauge
	labels []string
}

func NewGaugeVec(name, help string, labels []string) *GaugeVec {
	return &GaugeVec{
		name:   name,
		help:   help,
		gauges: make(map[string]*metricsGauge),
		labels: labels,
	}
}

func (v *GaugeVec) WithLabels(labels ...string) Gauge {
	key := v.makeLabelKey(labels)

	v.mu.Lock()
	defer v.mu.Unlock()

	if gauge, ok := v.gauges[key]; ok {
		return gauge
	}

	gauge := &metricsGauge{
		vec:    v,
		labels: v.labelsToMap(labels),
	}
	v.gauges[key] = gauge
	return gauge
}

func (v *GaugeVec) makeLabelKey(labels []string) string {
	return strings.Join(labels, ",")
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

func (v *GaugeVec) GetAll() map[string]float64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := make(map[string]float64)
	for key, gauge := range v.gauges {
		result[fmt.Sprintf("%s{%s}", v.name, key)] = gauge.Get()
	}
	return result
}

type HistogramVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	histograms map[string]*metricsHistogram
	labels     []string
	buckets    []float64
}

func NewHistogramVec(name, help string, labels []string, buckets []float64) *HistogramVec {
	if buckets == nil {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	return &HistogramVec{
		name:       name,
		help:       help,
		histograms: make(map[string]*metricsHistogram),
		labels:     labels,
		buckets:    buckets,
	}
}

func (v *HistogramVec) WithLabels(labels ...string) Histogram {
	key := v.makeLabelKey(labels)

	v.mu.Lock()
	defer v.mu.Unlock()

	if hist, ok := v.histograms[key]; ok {
		return hist
	}

	hist := &metricsHistogram{
		vec:          v,
		labels:       v.labelsToMap(labels),
		buckets:      v.buckets,
		bucketCounts: make([]uint64, len(v.buckets)),
	}
	v.histograms[key] = hist
	return hist
}

func (v *HistogramVec) makeLabelKey(labels []string) string {
	return strings.Join(labels, ",")
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

type Metrics struct {
	mu                sync.RWMutex
	counters          map[string]*CounterVec
	gauges            map[string]*GaugeVec
	histograms        map[string]*HistogramVec
	countersList      []*CounterVec
	gaugesList        []*GaugeVec
	histogramsList    []*HistogramVec
}

func New() *Metrics {
	return &Metrics{
		counters:   make(map[string]*CounterVec),
		gauges:     make(map[string]*GaugeVec),
		histograms: make(map[string]*HistogramVec),
	}
}

func (m *Metrics) NewCounter(name, help string, labels ...string) *CounterVec {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := fmt.Sprintf("%s_%s", MetricsNamespace, name)
	vec := NewCounterVec(fullName, help, labels)
	m.counters[fullName] = vec
	m.countersList = append(m.countersList, vec)
	return vec
}

func (m *Metrics) NewGauge(name, help string, labels ...string) *GaugeVec {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := fmt.Sprintf("%s_%s", MetricsNamespace, name)
	vec := NewGaugeVec(fullName, help, labels)
	m.gauges[fullName] = vec
	m.gaugesList = append(m.gaugesList, vec)
	return vec
}

func (m *Metrics) NewHistogram(name, help string, labels ...string) *HistogramVec {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullName := fmt.Sprintf("%s_%s", MetricsNamespace, name)
	vec := NewHistogramVec(fullName, help, labels, nil)
	m.histograms[fullName] = vec
	m.histogramsList = append(m.histogramsList, vec)
	return vec
}

func (m *Metrics) GetCounter(name string) *CounterVec {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counters[name]
}

func (m *Metrics) GetGauge(name string) *GaugeVec {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gauges[name]
}

func (m *Metrics) GetHistogram(name string) *HistogramVec {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.histograms[name]
}

func (m *Metrics) Describe() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	descs := make([]string, 0, len(m.counters)+len(m.gauges)+len(m.histograms))
	for name := range m.counters {
		descs = append(descs, name)
	}
	for name := range m.gauges {
		descs = append(descs, name)
	}
	for name := range m.histograms {
		descs = append(descs, name)
	}
	sort.Strings(descs)
	return descs
}

func (m *Metrics) Collect(ch chan<- prometheusMetric) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, counter := range m.countersList {
		counter.mu.RLock()
		for key, c := range counter.counters {
			if key == "" {
				ch <- prometheusMetric{
					name:   counter.name,
					help:   counter.help,
					Type:   CounterType,
					values: map[string]float64{"": c.value},
				}
			} else {
				labels := strings.Split(key, ",")
				for _, label := range labels {
					parts := strings.SplitN(label, "=", 2)
					if len(parts) == 2 {
						ch <- prometheusMetric{
							name:   counter.name,
							help:   counter.help,
							Type:   CounterType,
							values: map[string]float64{key: c.value},
						}
						break
					}
				}
			}
		}
		counter.mu.RUnlock()
	}

	for _, gauge := range m.gaugesList {
		gauge.mu.RLock()
		for key, g := range gauge.gauges {
			ch <- prometheusMetric{
				name:   gauge.name,
				help:   gauge.help,
				Type:   GaugeType,
				values: map[string]float64{key: g.value},
			}
		}
		gauge.mu.RUnlock()
	}

	for _, hist := range m.histogramsList {
		hist.mu.RLock()
		for _, h := range hist.histograms {
			values := make(map[string]float64)
			values["sum"] = h.sum
			values["count"] = float64(h.count)
			for i, bucket := range h.buckets {
				values[fmt.Sprintf("bucket:%f", bucket)] = float64(h.bucketCounts[i])
			}
			ch <- prometheusMetric{
				name:   hist.name,
				help:   hist.help,
				Type:   HistogramType,
				values: values,
			}
		}
		hist.mu.RUnlock()
	}
}

type MetricType string

const (
	CounterType   MetricType = "counter"
	GaugeType     MetricType = "gauge"
	HistogramType MetricType = "histogram"
	SummaryType   MetricType = "summary"
	UntypedType   MetricType = "untyped"
)

type prometheusMetric struct {
	name   string
	help   string
	Type   MetricType
	values map[string]float64
}

func (m *Metrics) OutputPrometheus(w io.Writer) {
	ch := make(chan prometheusMetric, 100)
	go func() {
		m.Collect(ch)
		close(ch)
	}()

	fmt.Fprintf(w, "# HELP %s Generated by DataPipe metrics exporter\n", MetricsNamespace)
	fmt.Fprintf(w, "# TYPE %s %s\n", MetricsNamespace, UntypedType)

	var metrics []prometheusMetric
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].name < metrics[j].name
	})

	for _, metric := range metrics {
		if metric.Type == CounterType {
			fmt.Fprintf(w, "# HELP %s %s\n", metric.name, metric.help)
			fmt.Fprintf(w, "# TYPE %s %s\n", metric.name, CounterType)
		} else if metric.Type == GaugeType {
			fmt.Fprintf(w, "# HELP %s %s\n", metric.name, metric.help)
			fmt.Fprintf(w, "# TYPE %s %s\n", metric.name, GaugeType)
		} else if metric.Type == HistogramType {
			fmt.Fprintf(w, "# HELP %s %s\n", metric.name, metric.help)
			fmt.Fprintf(w, "# TYPE %s %s\n", metric.name, HistogramType)
		}

		var keys []string
		for k := range metric.values {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			if k == "" {
				fmt.Fprintf(w, "%s %f\n", metric.name, metric.values[k])
			} else if strings.HasPrefix(k, "bucket:") {
				bucketVal := strings.TrimPrefix(k, "bucket:")
				fmt.Fprintf(w, "%s{%s} %f\n", metric.name, bucketVal, metric.values[k])
			} else {
				fmt.Fprintf(w, "%s %f\n", metric.name, metric.values[k])
			}
		}
	}
}

var globalMetrics *Metrics
var metricsOnce sync.Once

func GetMetrics() *Metrics {
	if globalMetrics == nil {
		metricsOnce.Do(func() {
			globalMetrics = New()
		})
	}
	return globalMetrics
}

type MetricsServer struct {
	metrics  *Metrics
	server   *http.Server
	addr     string
	path     string
}

func NewMetricsServer(addr string, path string) *MetricsServer {
	return &MetricsServer{
		metrics: GetMetrics(),
		addr:    addr,
		path:    path,
	}
}

func (s *MetricsServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.metrics.OutputPrometheus(w)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s.server.ListenAndServe()
}

func (s *MetricsServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func RegisterDefaultMetrics() *Metrics {
	m := GetMetrics()

	m.NewCounter(TaskStartedTotal, "Total number of tasks started", LabelService, LabelPipeline, LabelTask)
	m.NewCounter(TaskCompletedTotal, "Total number of tasks completed", LabelService, LabelPipeline, LabelTask, LabelStatus)
	m.NewCounter(TaskFailedTotal, "Total number of tasks failed", LabelService, LabelPipeline, LabelTask)
	m.NewCounter(TaskRetryTotal, "Total number of task retries", LabelService, LabelPipeline, LabelTask)
	m.NewHistogram(TaskDurationSeconds, "Task duration in seconds", LabelService, LabelPipeline, LabelTask, LabelStatus, LabelType)
	m.NewGauge(TaskQueueSize, "Current task queue size", LabelService)
	m.NewGauge(TaskProcessingSize, "Current task processing size", LabelService)

	m.NewCounter(PipelineCreatedTotal, "Total number of pipelines created", LabelService, LabelPipeline, LabelType)
	m.NewGauge(PipelineRunningTotal, "Current number of running pipelines", LabelService, LabelPipeline)
	m.NewCounter(PipelineCompletedTotal, "Total number of pipelines completed", LabelService, LabelPipeline, LabelStatus)
	m.NewCounter(PipelineFailedTotal, "Total number of pipelines failed", LabelService, LabelPipeline)
	m.NewHistogram(PipelineDurationSeconds, "Pipeline duration in seconds", LabelService, LabelPipeline, LabelStatus)

	m.NewCounter(NodeProcessedTotal, "Total number of nodes processed", LabelService, LabelPipeline, LabelNode, LabelType)
	m.NewCounter(NodeFailedTotal, "Total number of nodes failed", LabelService, LabelPipeline, LabelNode, LabelType)
	m.NewHistogram(NodeDurationSeconds, "Node duration in seconds", LabelService, LabelPipeline, LabelNode, LabelType)
	m.NewGauge(NodeQueueDepth, "Current node queue depth", LabelService, LabelPipeline, LabelNode)
	m.NewCounter(NodeThroughputBytes, "Node throughput in bytes", LabelService, LabelPipeline, LabelNode, LabelType)

	m.NewCounter(WorkerRegisteredTotal, "Total number of workers registered", LabelService, LabelWorker)
	m.NewCounter(WorkerHeartbeatTotal, "Total number of worker heartbeats", LabelService, LabelWorker)
	m.NewCounter(WorkerLostTotal, "Total number of workers lost", LabelService, LabelWorker)
	m.NewGauge(WorkerTaskCapacity, "Worker task capacity", LabelService, LabelWorker)
	m.NewGauge(WorkerActiveTasks, "Worker active tasks", LabelService, LabelWorker)

	m.NewCounter(DatabaseQueryTotal, "Total number of database queries", LabelService, LabelOperation)
	m.NewHistogram(DatabaseQueryDuration, "Database query duration in seconds", LabelService, LabelOperation)
	m.NewGauge(DatabaseConnectionPool, "Database connection pool size", LabelService)
	m.NewGauge(DatabaseConnectionInUse, "Database connections in use", LabelService)

	m.NewCounter(RedisOperationTotal, "Total number of Redis operations", LabelService, LabelOperation, LabelStatus)
	m.NewHistogram(RedisOperationDuration, "Redis operation duration in seconds", LabelService, LabelOperation)

	m.NewCounter(HTTPRequestTotal, "Total number of HTTP requests", LabelService, LabelOperation, LabelStatus)
	m.NewHistogram(HTTPRequestDuration, "HTTP request duration in seconds", LabelService, LabelOperation, LabelStatus)

	return m
}

var _ Counter = (*metricsCounter)(nil)
var _ Gauge = (*metricsGauge)(nil)
var _ Histogram = (*metricsHistogram)(nil)
