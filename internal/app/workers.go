// Package app
//
// workers.go contains wiring logic for building ETL workers.
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

	"github.com/nisemenov/etl_service/internal/config"
	"github.com/nisemenov/etl_service/internal/consumer"
	"github.com/nisemenov/etl_service/internal/etl"
	"github.com/nisemenov/etl_service/internal/httpclient"
	"github.com/nisemenov/etl_service/internal/producer"
	"github.com/nisemenov/etl_service/internal/repository"
	"github.com/nisemenov/etl_service/internal/worker"
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
			map[string]string{"Authorization": cfg.AuthToken},
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

	// etl piplines
	paymentEtl := etl.NewETLPipline(
		producer.NewPaymentProducer(apiClient, logger.With("component", "payment producer")),
		repository.NewSQLitePaymentRepo(db, logger.With("component", "payment repository")),
		paymentConsumer,
		logger.With("component", "payment etl"),
	)
	yooPaymentEtl := etl.NewETLPipline(
		producer.NewYooPaymentProducer(apiClient, logger.With("component", "yookassa payment producer")),
		repository.NewSQLiteYooPaymentRepo(db, logger.With("component", "yookassa payment repository")),
		yooPaymentConsumer,
		logger.With("component", "yookassa payment etl"),
	)

	// workers
	return []*worker.Worker{
		worker.NewWorker(paymentEtl, 30*time.Minute, logger.With("component", "payment worker")),
		worker.NewWorker(yooPaymentEtl, 30*time.Minute, logger.With("component", "yookassa payment worker")),
	}
}
