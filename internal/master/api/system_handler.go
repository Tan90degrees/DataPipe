package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"datapipe/internal/common/logging"
	"datapipe/internal/master/config"
)

type SystemHandler struct {
	db  *gorm.DB
	cfg *config.Config
}

func NewSystemHandler(db *gorm.DB, cfg *config.Config) *SystemHandler {
	return &SystemHandler{
		db:  db,
		cfg: cfg,
	}
}

type HealthResponse struct {
	Status    string                 `json:"status"`
	Timestamp string                 `json:"timestamp"`
	Components map[string]string     `json:"components,omitempty"`
}

type SystemMetrics struct {
	Uptime        int64               `json:"uptime_seconds"`
	MemoryUsage   MemoryMetrics       `json:"memory_usage"`
	CPUUsage      float64             `json:"cpu_usage_percent"`
	GoVersion     string              `json:"go_version"`
	 Goroutines   int                 `json:"goroutines"`
}

type MemoryMetrics struct {
	Alloc      uint64 `json:"alloc_bytes"`
	TotalAlloc uint64 `json:"total_alloc_bytes"`
	Sys        uint64 `json:"sys_bytes"`
}

type NodeInfo struct {
	ID        string `json:"id"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Status    string `json:"status"`
	Load      int    `json:"load"`
	UpdatedAt string `json:"updated_at"`
}

func (h *SystemHandler) Health(c *gin.Context) {
	components := make(map[string]string)

	if err := h.db.Exec("SELECT 1").Error; err != nil {
		components["database"] = "unhealthy"
	} else {
		components["database"] = "healthy"
	}

	status := "healthy"
	for _, compStatus := range components {
		if compStatus == "unhealthy" {
			status = "degraded"
			break
		}
	}

	c.JSON(http.StatusOK, HealthResponse{
		Status:     status,
		Components: components,
	})
}

func (h *SystemHandler) Metrics(c *gin.Context) {
	var executionCounts map[string]int64

	sql := "SELECT status, COUNT(*) as count FROM executions GROUP BY status"
	rows, err := h.db.Raw(sql).Rows()
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to get execution metrics: %v", err))
	} else {
		executionCounts = make(map[string]int64)
		defer rows.Close()
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err == nil {
				executionCounts[status] = count
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"executions": executionCounts,
	})
}

func (h *SystemHandler) Nodes(c *gin.Context) {
	nodes := []NodeInfo{
		{
			ID:     "node-1",
			Host:   "localhost",
			Port:   8081,
			Status: "online",
			Load:   30,
		},
		{
			ID:     "node-2",
			Host:   "localhost",
			Port:   8082,
			Status: "online",
			Load:   45,
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"nodes": nodes,
		"total": len(nodes),
	})
}

func (h *SystemHandler) Config(c *gin.Context) {
	cfg := h.cfg.Master
	c.JSON(http.StatusOK, gin.H{
		"host": cfg.Host,
		"port": cfg.Port,
	})
}

func GinLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
	}
}
