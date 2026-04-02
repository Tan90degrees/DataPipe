package monitoring

import (
	"runtime"
	"sync"
	"time"
)

type Collector struct {
	mu           sync.RWMutex
	registry     *MetricsRegistry
	interval     time.Duration
	stopCh       chan struct{}
	wg           sync.WaitGroup
	host         string
	instance     string
}

func NewCollector(host, instance string, interval time.Duration) *Collector {
	if interval == 0 {
		interval = 10 * time.Second
	}

	return &Collector{
		registry: GetRegistry(),
		interval: interval,
		stopCh:   make(chan struct{}),
		host:     host,
		instance: instance,
	}
}

func (c *Collector) Start() {
	c.wg.Add(1)
	go c.collectLoop()
}

func (c *Collector) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *Collector) collectLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	c.collectSystemMetrics()

	for {
		select {
		case <-ticker.C:
			c.collectSystemMetrics()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Collector) collectSystemMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	c.registry.Resource().SetMemoryUsage(c.host, c.instance, float64(memStats.Alloc))

	numCPU := runtime.NumCPU()
	c.registry.Resource().SetCPUUsage(c.host, c.instance, 0)
	_ = numCPU
}

type WorkerHeartbeat struct {
	WorkerID      string    `json:"worker_id"`
	Timestamp     time.Time `json:"timestamp"`
	TasksRunning   int       `json:"tasks_running"`
	TasksCapacity int       `json:"tasks_capacity"`
	CPUUsage      float64   `json:"cpu_usage"`
	MemoryUsage   float64   `json:"memory_usage"`
	Status        string    `json:"status"`
}

type HeartbeatCollector struct {
	mu          sync.RWMutex
	registry    *MetricsRegistry
	heartbeats  map[string]*WorkerHeartbeat
	interval    time.Duration
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func NewHeartbeatCollector(interval time.Duration) *HeartbeatCollector {
	if interval == 0 {
		interval = 5 * time.Second
	}

	return &HeartbeatCollector{
		registry:   GetRegistry(),
		heartbeats: make(map[string]*WorkerHeartbeat),
		interval:   interval,
		stopCh:     make(chan struct{}),
	}
}

func (hc *HeartbeatCollector) Start() {
	hc.wg.Add(1)
	go hc.heartbeatLoop()
}

func (hc *HeartbeatCollector) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

func (hc *HeartbeatCollector) heartbeatLoop() {
	defer hc.wg.Done()

	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.processHeartbeats()
		case <-hc.stopCh:
			return
		}
	}
}

func (hc *HeartbeatCollector) processHeartbeats() {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	now := time.Now()
	for workerID, hb := range hc.heartbeats {
		age := now.Sub(hb.Timestamp)
		if age > 30*time.Second {
			hc.registry.Node().SetNodeStatus(workerID, "unknown", 0)
		} else {
			status := 1.0
			if hb.Status == "inactive" || hb.Status == "lost" {
				status = 0.0
			}
			hc.registry.Node().SetNodeStatus(workerID, "unknown", status)
			hc.registry.Node().SetTasksRunning(workerID, "unknown", float64(hb.TasksRunning))

			hc.registry.Resource().SetCPUUsage(hc.hostname(), workerID, hb.CPUUsage)
			hc.registry.Resource().SetMemoryUsage(hc.hostname(), workerID, hb.MemoryUsage)
		}
	}
}

func (hc *HeartbeatCollector) hostname() string {
	return "worker"
}

func (hc *HeartbeatCollector) ReceiveHeartbeat(heartbeat *WorkerHeartbeat) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	heartbeat.Timestamp = time.Now()
	hc.heartbeats[heartbeat.WorkerID] = heartbeat
}

func (hc *HeartbeatCollector) RemoveWorker(workerID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	delete(hc.heartbeats, workerID)
}

func (hc *HeartbeatCollector) GetHeartbeat(workerID string) (*WorkerHeartbeat, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	hb, ok := hc.heartbeats[workerID]
	return hb, ok
}

func (hc *HeartbeatCollector) ListWorkers() []*WorkerHeartbeat {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	result := make([]*WorkerHeartbeat, 0, len(hc.heartbeats))
	for _, hb := range hc.heartbeats {
		result = append(result, hb)
	}
	return result
}

type SystemCollector struct {
	mu        sync.RWMutex
	registry  *MetricsRegistry
	interval  time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup
	host      string
	instance  string
}

func NewSystemCollector(host, instance string, interval time.Duration) *SystemCollector {
	if interval == 0 {
		interval = 15 * time.Second
	}

	return &SystemCollector{
		registry:  GetRegistry(),
		interval:  interval,
		stopCh:    make(chan struct{}),
		host:      host,
		instance:  instance,
	}
}

func (sc *SystemCollector) Start() {
	sc.wg.Add(1)
	go sc.collectLoop()
}

func (sc *SystemCollector) Stop() {
	close(sc.stopCh)
	sc.wg.Wait()
}

func (sc *SystemCollector) collectLoop() {
	defer sc.wg.Done()

	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sc.collect()
		case <-sc.stopCh:
			return
		}
	}
}

func (sc *SystemCollector) collect() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	sc.registry.Resource().SetMemoryUsage(sc.host, sc.instance, float64(memStats.Alloc))

	var stat runtime.MemStats
	runtime.ReadMemStats(&stat)
	_ = stat
}

type MetricsCollectorConfig struct {
	Host              string
	Instance          string
	SystemInterval   time.Duration
	HeartbeatInterval time.Duration
}

func NewMetricsCollectorConfig() *MetricsCollectorConfig {
	return &MetricsCollectorConfig{
		Host:              "localhost",
		Instance:          "main",
		SystemInterval:   15 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	}
}

type MetricsCollector struct {
	systemCollector    *SystemCollector
	heartbeatCollector  *HeartbeatCollector
}

func NewMetricsCollector(cfg *MetricsCollectorConfig) *MetricsCollector {
	if cfg == nil {
		cfg = NewMetricsCollectorConfig()
	}

	return &MetricsCollector{
		systemCollector:   NewSystemCollector(cfg.Host, cfg.Instance, cfg.SystemInterval),
		heartbeatCollector: NewHeartbeatCollector(cfg.HeartbeatInterval),
	}
}

func (mc *MetricsCollector) Start() {
	mc.systemCollector.Start()
	mc.heartbeatCollector.Start()
}

func (mc *MetricsCollector) Stop() {
	mc.systemCollector.Stop()
	mc.heartbeatCollector.Stop()
}

func (mc *MetricsCollector) ReceiveHeartbeat(heartbeat *WorkerHeartbeat) {
	mc.heartbeatCollector.ReceiveHeartbeat(heartbeat)
}

func (mc *MetricsCollector) RemoveWorker(workerID string) {
	mc.heartbeatCollector.RemoveWorker(workerID)
}

func (mc *MetricsCollector) ListWorkers() []*WorkerHeartbeat {
	return mc.heartbeatCollector.ListWorkers()
}
