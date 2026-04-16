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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nisemenov/etl_service/internal/config"
	"github.com/nisemenov/etl_service/internal/handler"
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

	// HTTP server
	srv := handler.NewHTTPServer(a.cfg, a.logger)

	go func() {
		a.logger.Info("server started", "addr", a.cfg.HTTPAddr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.logger.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()

	a.logger.Info("shutdown signal received", "ctx cause", context.Cause(ctx))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.logger.Error("http shutdown failed", "err", err)
	}

	a.logger.Info("application stopped")
}
