package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"datapipe/internal/common/logging"
	"datapipe/internal/master/api"
	"datapipe/internal/master/config"
	"datapipe/internal/master/scheduler"
	"datapipe/internal/models"
)

var (
	configPath = flag.String("config", "config.yaml", "path to config file")
)

func main() {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := setupLogger(cfg); err != nil {
		fmt.Printf("Failed to setup logger: %v\n", err)
		os.Exit(1)
	}

	db, err := initDatabase(cfg)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to initialize database: %v", err))
		os.Exit(1)
	}

	if err := autoMigrate(db); err != nil {
		logging.Error(fmt.Sprintf("Failed to run database migrations: %v", err))
		os.Exit(1)
	}

	sched := scheduler.NewScheduler(db)
	if err := sched.Start(context.Background()); err != nil {
		logging.Error(fmt.Sprintf("Failed to start scheduler: %v", err))
		os.Exit(1)
	}

	server := api.NewServer(db, cfg)
	if err := server.Start(); err != nil {
		logging.Error(fmt.Sprintf("Server error: %v", err))
	}

	if err := sched.Stop(); err != nil {
		logging.Error(fmt.Sprintf("Failed to stop scheduler: %v", err))
	}

	sqlDB, err := db.DB()
	if err == nil {
		sqlDB.Close()
	}
}

func setupLogger(cfg *config.Config) error {
	l := logging.New("master")
	l.SetLevel(logging.ParseLevel(cfg.Logging.Level))
	logging.SetLogger(l)
	return nil
}

func initDatabase(cfg *config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=UTC",
		cfg.Database.Host,
		cfg.Database.Username,
		cfg.Database.Password,
		cfg.Database.Database,
		cfg.Database.Port,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return db, nil
}

func autoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&models.Pipeline{},
		&models.PipelineVersion{},
		&models.Function{},
		&models.Execution{},
		&models.Task{},
	)
}

func waitForSignal() os.Signal {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return <-quit
}
