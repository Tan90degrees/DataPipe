package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"datapipe/internal/common/logging"
	"datapipe/internal/master/config"
)

type Server struct {
	engine              *gin.Engine
	httpServer          *http.Server
	pipelineHandler     *PipelineHandler
	functionHandler    *FunctionHandler
	executionHandler   *ExecutionHandler
	systemHandler      *SystemHandler
	masterConfig       config.MasterConfig
}

func NewServer(db *gorm.DB, cfg *config.Config) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	engine.Use(gin.Recovery())
	engine.Use(GinLogger())
	engine.Use(corsMiddleware())

	pipelineHandler := NewPipelineHandler(db)
	functionHandler := NewFunctionHandler(db)
	executionHandler := NewExecutionHandler(db)
	systemHandler := NewSystemHandler(db, cfg)

	server := &Server{
		engine:           engine,
		pipelineHandler:  pipelineHandler,
		functionHandler:  functionHandler,
		executionHandler: executionHandler,
		systemHandler:    systemHandler,
		masterConfig:     cfg.Master,
	}

	server.setupRoutes()

	addr := cfg.Master.GetAddr()
	server.httpServer = &http.Server{
		Addr:         addr,
		Handler:      engine,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return server
}

func (s *Server) setupRoutes() {
	uiPath := os.Getenv("UI_PATH")
	if uiPath == "" {
		uiPath = "./ui"
	}

	v1 := s.engine.Group("/api/v1")
	{
		system := v1.Group("/system")
		{
			system.GET("/health", s.systemHandler.Health)
			system.GET("/metrics", s.systemHandler.Metrics)
			system.GET("/nodes", s.systemHandler.Nodes)
			system.GET("/config", s.systemHandler.Config)
		}

		pipelines := v1.Group("/pipelines")
		{
			pipelines.POST("", s.pipelineHandler.Create)
			pipelines.GET("", s.pipelineHandler.List)
			pipelines.GET("/:id", s.pipelineHandler.Get)
			pipelines.PUT("/:id", s.pipelineHandler.Update)
			pipelines.DELETE("/:id", s.pipelineHandler.Delete)
			pipelines.POST("/:id/start", s.pipelineHandler.Start)
			pipelines.POST("/:id/stop", s.pipelineHandler.Stop)
			pipelines.POST("/:id/pause", s.pipelineHandler.Pause)
			pipelines.POST("/:id/resume", s.pipelineHandler.Resume)
			pipelines.GET("/:id/versions", s.pipelineHandler.Versions)
		}

		functions := v1.Group("/functions")
		{
			functions.POST("", s.functionHandler.Create)
			functions.GET("", s.functionHandler.List)
			functions.GET("/:name", s.functionHandler.Get)
			functions.DELETE("/:name", s.functionHandler.Delete)
			functions.POST("/:name/validate", s.functionHandler.Validate)
		}

		executions := v1.Group("/executions")
		{
			executions.GET("", s.executionHandler.List)
			executions.GET("/:id", s.executionHandler.Get)
			executions.GET("/:id/logs", s.executionHandler.Logs)
			executions.GET("/:id/metrics", s.executionHandler.Metrics)
		}
	}

	s.engine.Static("/static", uiPath+"/static")
	s.engine.StaticFile("/", uiPath+"/index.html")
	s.engine.NoRoute(func(c *gin.Context) {
		c.File(uiPath + "/index.html")
	})
}

func (s *Server) Start() error {
	go func() {
		logging.Info(fmt.Sprintf("Starting Master server on %s", s.httpServer.Addr))
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.Error(fmt.Sprintf("Failed to start server: %v", err))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logging.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	logging.Info("Server exited gracefully")
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
