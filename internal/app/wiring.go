// Package app
//
// contains wiring logic for building ETL workers.
//
// The file is responsible for creating producers, repositories,
// consumers and assembling them into ETL pipelines that are
// executed by background workers.
package app

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/nisemenov/etl-service/internal/config"
	"github.com/nisemenov/etl-service/internal/consumer"
	"github.com/nisemenov/etl-service/internal/etl"
	"github.com/nisemenov/etl-service/internal/httpclient"
	"github.com/nisemenov/etl-service/internal/producer"
	"github.com/nisemenov/etl-service/internal/repository"
	"github.com/nisemenov/etl-service/internal/worker"
)

func buildWorkers(cfg *config.Config, logger *slog.Logger, db *sql.DB) []*worker.Worker {
	// HTTP clients
	baseHTTP := &http.Client{
		Timeout: 60 * time.Second,
	}
	apiClient := httpclient.NewHTTPClient(
		baseHTTP,
		cfg.APIBaseURL,
		logger.With("component", "httpclient"),
		httpclient.WithHeaders(
			map[string]string{"X-Internal-Token": cfg.XInternalToken},
		),
	)
	chClient := httpclient.NewHTTPClient(
		baseHTTP,
		fmt.Sprintf("http://%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort),
		logger.With("component", "httpclient"),
		httpclient.WithHeaders(
			map[string]string{"X-ClickHouse-Database": cfg.ClickHouseDB}, // default db
		),
		httpclient.WithMiddleware(
			func(r *http.Request) { r.SetBasicAuth(cfg.ClickHouseUser, cfg.ClickHousePassword) },
		),
	)

	// CH consumers
	paymentConsumer := consumer.NewClickHouseLoader(
		chClient,
		cfg.PaymentCHTableName,
		consumer.PaymentToClickHouseRow,
		logger.With("component", "payment consumer"),
	)
	yooPaymentConsumer := consumer.NewClickHouseLoader(
		chClient,
		cfg.YooPaymentCHTableName,
		consumer.YooPaymentToClickHouseRow,
		logger.With("component", "yookassa payment consumer"),
	)

	// repos
	paymentRepo := repository.NewSQLitePaymentRepo(db, logger.With("component", "payment repository"))
	yooPaymentRepo := repository.NewSQLiteYooPaymentRepo(db, logger.With("component", "yookassa payment repository"))

	// etl pipelines
	paymentEtl := etl.NewETLPipeline(
		producer.NewPaymentProducer(apiClient, logger.With("component", "payment producer")),
		paymentRepo,
		paymentConsumer,
		logger.With("component", "payment etl"),
	)
	yooPaymentEtl := etl.NewETLPipeline(
		producer.NewYooPaymentProducer(apiClient, logger.With("component", "yookassa payment producer")),
		yooPaymentRepo,
		yooPaymentConsumer,
		logger.With("component", "yookassa payment etl"),
	)

	return []*worker.Worker{
		// main etl workers
		worker.NewWorker(paymentEtl, 30*time.Minute, logger.With("component", "payment worker")),
		worker.NewWorker(yooPaymentEtl, 30*time.Minute, logger.With("component", "yookassa payment worker")),
		// requeue job workers
		worker.NewWorker(
			worker.JobFunc(paymentRepo.RequeueStaleProcessing),
			10*time.Minute,
			logger.With("component", "payment requeue worker"),
		),
		worker.NewWorker(
			worker.JobFunc(yooPaymentRepo.RequeueStaleProcessing),
			10*time.Minute,
			logger.With("component", "yookassa payment requeue worker"),
		),
		// cleanup job workers
		worker.NewWorker(
			worker.JobFunc(paymentRepo.DeleteExported),
			7*24*time.Hour,
			logger.With("component", "payment delete exported worker"),
		),
		worker.NewWorker(
			worker.JobFunc(yooPaymentRepo.DeleteExported),
			7*24*time.Hour,
			logger.With("component", "yookassa payment delete exported worker"),
		),
	}
}
