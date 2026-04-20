// Package etl coordinates the end-to-end ETL workflow,
// orchestrating producers, repositories, and workers.
package etl

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type etlPipeline[ID comparable, D any] struct {
	producer Producer[ID, D]
	repo     Repository[ID, D]
	consumer Consumer[D]
	logger   *slog.Logger
}

func (etl *etlPipeline[ID, D]) Run(ctx context.Context) error {
	start := time.Now()

	fetched, inserted, err := etl.fetch(ctx)
	if err != nil {
		etl.logger.Error("etl run failed",
			"stage", "fetch",
			"err", err,
		)
		return err
	}

	processed, err := etl.process(ctx)
	if err != nil {
		etl.logger.Error("etl run failed",
			"stage", "process",
			"err", err,
		)
		return err
	}

	ack, err := etl.acknowledge(ctx)
	if err != nil {
		etl.logger.Error("etl run failed",
			"stage", "acknowledge",
			"err", err,
		)
		return err
	}

	etl.logger.Info("etl run completed",
		"fetched", fetched,
		"inserted", inserted,
		"duplicates", fetched-inserted,
		"processed", processed,
		"acknowledged", ack,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	return nil
}

func (etl *etlPipeline[ID, D]) fetch(ctx context.Context) (int, int, error) {
	instances, err := etl.producer.Fetch(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("producer fetch failed: %w", err)
	}
	if len(instances) == 0 {
		return 0, 0, nil
	}

	inserted, err := etl.repo.SaveBatch(ctx, instances)
	if err != nil {
		return 0, 0, fmt.Errorf("repository save failed: %w", err)
	}

	return len(instances), inserted, nil
}

// process stage provides at-least-once delivery semantics.
//
// Failure scenarios:
//
// 1. If failure happens BEFORE InsertBatch:
//   - data is not written to the consumer
//   - safe to retry (no duplicates)
//
// 2. If failure happens AFTER InsertBatch but BEFORE MarkStatus(StatusSent):
//   - data may already be written to the consumer
//   - retry will cause duplicate processing
//
// Therefore, retries MUST be idempotent on the consumer side.
func (etl *etlPipeline[ID, D]) process(ctx context.Context) (int, error) {
	batch, err := etl.repo.FetchForProcessing(ctx)
	if err != nil {
		return 0, fmt.Errorf("repository fetch failed: %w", err)
	}

	if batch == nil {
		return 0, nil
	}

	if err := etl.consumer.InsertBatch(ctx, batch.Items); err != nil {
		return 0, fmt.Errorf("clickhouse insert failed: %w", err)
	}

	if err := etl.repo.MarkStatus(ctx, batch.IDs, StatusSent); err != nil {
		return 0, fmt.Errorf("mark sent failed: %w", err)
	}

	return len(batch.Items), nil
}

func (etl *etlPipeline[ID, D]) acknowledge(ctx context.Context) (int, error) {
	batch, err := etl.repo.FetchByStatus(ctx, StatusSent)
	if err != nil {
		return 0, fmt.Errorf("repository fetch failed: %w", err)
	}

	if batch == nil || len(batch.IDs) == 0 {
		return 0, nil
	}

	if err := etl.producer.Acknowledge(ctx, batch.IDs); err != nil {
		return 0, fmt.Errorf("ack failed: %w", err)
	}

	if err := etl.repo.MarkStatus(ctx, batch.IDs, StatusExported); err != nil {
		return 0, fmt.Errorf("mark exported failed: %w", err)
	}

	return len(batch.IDs), nil
}

func NewETLPipeline[ID comparable, D any](
	producer Producer[ID, D],
	repo Repository[ID, D],
	consumer Consumer[D],
	logger *slog.Logger,
) *etlPipeline[ID, D] {
	return &etlPipeline[ID, D]{producer: producer, repo: repo, consumer: consumer, logger: logger}
}
