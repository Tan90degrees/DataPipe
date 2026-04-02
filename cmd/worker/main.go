package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"datapipe/internal/common/logging"
	"datapipe/internal/worker"
	"datapipe/internal/worker/config"
	"datapipe/internal/worker/function"
)

var (
	configPath = flag.String("config", "config.yaml", "path to worker config file")
	showHelp   = flag.Bool("help", false, "show help message")
	showVersion = flag.Bool("version", false, "show version")
)

const version = "1.0.0"

func main() {
	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Printf("DataPipe Worker v%s\n", version)
		os.Exit(0)
	}

	logger := logging.GetLogger()
	logger.Info("starting datapipe worker", logging.Fields{
		"version": version,
		"config":  *configPath,
	})

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", logging.Fields{
			"error": err.Error(),
			"path":  *configPath,
		})
		os.Exit(1)
	}

	initLogger(cfg)

	w, err := worker.New(cfg)
	if err != nil {
		logger.Error("failed to create worker", logging.Fields{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	registerBuiltinFunctions(w)

	if err := w.Start(); err != nil {
		logger.Error("failed to start worker", logging.Fields{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	logger.Info("worker started successfully", logging.Fields{
		"worker_id": w.GetID(),
		"worker_name": w.GetName(),
		"address":  cfg.Worker.GetAddr(),
	})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	logger.Info("shutting down worker...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		if err := w.Stop(); err != nil {
			logger.Error("failed to stop worker", logging.Fields{
				"error": err.Error(),
			})
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		logger.Warn("shutdown timed out, forcing exit")
	case <-done:
		logger.Info("worker shutdown complete")
	}
}

func initLogger(cfg *config.Config) {
	loggerConfig := logging.LoggerConfig{
		Level:      cfg.Logging.Level,
		Format:     cfg.Logging.Format,
		OutputPath: cfg.Logging.OutputPath,
		Service:    "worker",
	}

	newLogger, err := logging.NewLoggerWithConfig(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		return
	}

	if newLogger != nil {
		_ = newLogger
	}
}

func registerBuiltinFunctions(w *worker.Worker) {
	scanFn := function.NewStartFunction("file_scanner", "v1", map[string]interface{}{
		"directory": ".",
		"recursive": true,
		"file_types": []interface{}{".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".md", ".html", ".htm", ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp"},
	})
	w.RegisterFunction(scanFn)

	identityFn := function.NewNormalFunction("identity", "v1", nil)
	w.RegisterFunction(identityFn)

	collectFn := function.NewEndFunction("collector", "v1", nil)
	w.RegisterFunction(collectFn)
}

func waitForShutdown(timeout time.Duration) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timed out after %v", timeout)
	}
}
