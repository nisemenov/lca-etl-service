package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nisemenov/etl_service/internal/config"
	"github.com/nisemenov/etl_service/internal/consumer"
	"github.com/nisemenov/etl_service/internal/etl"
	"github.com/nisemenov/etl_service/internal/httpclient"
	"github.com/nisemenov/etl_service/internal/producer"
	"github.com/nisemenov/etl_service/internal/repository"
	"github.com/nisemenov/etl_service/internal/storage/sqlite"
	"github.com/nisemenov/etl_service/internal/worker"
)

const scheduleCron = 1 * time.Minute

func newLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		// AddSource: debug,
	}

	if debug {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func main() {
	cfg := config.Load()

	// init Logger
	logger := newLogger(cfg.Debug)
	slog.SetDefault(logger)

	logger.Info(
		"starting application",
		"version", "v0.0.1",
		"debug", cfg.Debug,
	)

	db := sqlite.OpenSQLite(cfg.DBPath)
	sqlite.Migrate(db)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// graceful shutdown
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		logger.Info("shutting down...")
		cancel()
	}()

	// HTTP Clients
	baseHTTP := &http.Client{
		Timeout: 60 * time.Second,
	}
	apiClient := httpclient.NewHTTPClient(baseHTTP, cfg.APIBaseURL, logger.With("component", "httpclient"))
	chClient := httpclient.NewHTTPClient(
		baseHTTP,
		fmt.Sprintf("http://%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort),
		logger.With("component", "httpclient"),
		httpclient.WithHeaders(
			map[string]string{"X-ClickHouse-Database": cfg.ClickHouseDB},
		),
		httpclient.WithMiddleware(
			func(r *http.Request) { r.SetBasicAuth(cfg.ClickHouseUser, cfg.ClickHousePassword) },
		),
	)

	// piplines
	etlPayment := etl.NewETLPipline(
		producer.NewPaymentProducer(apiClient, logger.With("component", "payment producer")),
		repository.NewSQLitePaymentRepo(db, logger.With("component", "payment repository")),
		consumer.NewClickHouseLoader(chClient, "short_url_tasks", logger.With("component", "payment consumer")),
		logger.With("component", "payment etl"),
	)
	workerPayment := worker.NewWorker(etlPayment, scheduleCron, logger.With("component", "payment worker"))
	workerPayment.Run(ctx)
}
