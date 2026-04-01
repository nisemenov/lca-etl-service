// Package app contains the application composition root.
//
// It is responsible for initializing all dependencies such as
// configuration, database connections, HTTP clients, ETL pipelines
// and workers, and for managing the application lifecycle including
// graceful shutdown.
package app

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nisemenov/etl_service/internal/config"
	"github.com/nisemenov/etl_service/internal/storage/sqlite"
	"github.com/nisemenov/etl_service/internal/worker"
)

type App struct {
	cfg     *config.Config
	db      *sql.DB
	workers []*worker.Worker
	logger  *slog.Logger
}

func New(cfg *config.Config) *App {
	logger := newLogger(cfg.Debug)

	db := sqlite.InitSQLite(cfg.DBPath)

	workers := buildWorkers(cfg, logger, db)

	return &App{
		cfg:     cfg,
		db:      db,
		workers: workers,
		logger:  logger,
	}
}

func (a *App) Run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer a.db.Close()

	a.logger.Info(
		"starting application",
		"version", "0.0.1",
		"debug", a.cfg.Debug,
	)

	for _, w := range a.workers {
		go w.Run(ctx)
	}

	<-ctx.Done()

	a.logger.Info("shutdown signal received")

	time.Sleep(200 * time.Millisecond)

	a.logger.Info("application stopped")
}
