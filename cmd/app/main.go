package main

import (
	"context"
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

func main() {
	level := slog.LevelInfo
	cfg := config.Load()

	if cfg.Debug {
		level = slog.LevelDebug
	}

	// init Logger
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: cfg.Debug, // показываем source только при cnf.Debug == true
	})
	logger := slog.New(handler)
	slog.SetDefault(logger) // опционально, если хочешь использовать slog.Info() без переменной

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

	httpClient := httpclient.NewHTTPClient(&http.Client{}, cfg.APIBaseURL)

	producerPayment := producer.NewPaymentProducer(httpClient, logger)

	repoPayment := repository.NewSQLitePaymentRepo(db, logger)

	consumerPayment := consumer.NewClickHouseLoader(httpClient, "payments", logger)

	etlPayment := etl.NewETLPipline(producerPayment, repoPayment, consumerPayment, logger)

	workerPayment := worker.New(etlPayment, 5*time.Second, logger)

	workerPayment.Run(ctx)
}
