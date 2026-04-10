// Package etl coordinates the end-to-end ETL workflow,
// orchestrating producers, repositories, and workers.
package etl

import (
	"context"
	"fmt"
	"log/slog"
)

type etlPipeline[ID comparable, D any] struct {
	producer Producer[ID, D]
	repo     Repository[ID, D]
	consumer Consumer[D]
	logger   *slog.Logger
}

func (etl *etlPipeline[ID, D]) Run(ctx context.Context) error {
	if err := etl.fetch(ctx); err != nil {
		etl.logger.Error("fetch stage failed", "err", err)
		return err
	}

	if err := etl.process(ctx); err != nil {
		etl.logger.Error("process stage failed", "err", err)
		return err
	}

	if err := etl.acknowledge(ctx); err != nil {
		etl.logger.Error("ack stage failed", "err", err)
		return err
	}

	return nil
}

func (etl *etlPipeline[ID, D]) fetch(ctx context.Context) error {
	instances, err := etl.producer.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("producer fetch failed: %w", err)
	}
	if len(instances) == 0 {
		etl.logger.Info("no instances fetched from producer")
		return nil
	}

	if err := etl.repo.SaveBatch(ctx, instances); err != nil {
		return fmt.Errorf("repository save failed: %w", err)
	}

	etl.logger.Info("instances fetched and saved", "count", len(instances))
	return nil
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
func (etl *etlPipeline[ID, D]) process(ctx context.Context) error {
	batch, err := etl.repo.FetchForProcessing(ctx)
	if err != nil {
		return fmt.Errorf("repository fetch failed: %w", err)
	}

	if batch == nil {
		etl.logger.Info("no instances for processing")
		return nil
	}

	if err := etl.consumer.InsertBatch(ctx, batch.Items); err != nil {
		return fmt.Errorf("clickhouse insert failed: %w", err)
	}

	if err := etl.repo.MarkStatus(ctx, batch.IDs, StatusSent); err != nil {
		return fmt.Errorf("mark sent failed: %w", err)
	}

	etl.logger.Info("instances inserted into clickhouse", "count", len(batch.Items))
	return nil
}

func (etl *etlPipeline[ID, D]) acknowledge(ctx context.Context) error {
	batch, err := etl.repo.FetchByStatus(ctx, StatusSent)
	if err != nil {
		return fmt.Errorf("repository fetch failed: %w", err)
	}
	ids := batch.IDs

	if len(ids) == 0 {
		etl.logger.Info("no instances to acknowledge")
		return nil
	}

	if err = etl.producer.Acknowledge(ctx, ids); err != nil {
		return fmt.Errorf("payment acknowledge failed: %w", err)
	}

	if err := etl.repo.MarkStatus(ctx, ids, StatusExported); err != nil {
		return fmt.Errorf("mark exported failed: %w", err)
	}

	etl.logger.Info("payments acknowledged successfully", "count", len(ids))
	return nil
}

func NewETLPipeline[ID comparable, D any](
	producer Producer[ID, D],
	repo Repository[ID, D],
	consumer Consumer[D],
	logger *slog.Logger,
) *etlPipeline[ID, D] {
	return &etlPipeline[ID, D]{producer: producer, repo: repo, consumer: consumer, logger: logger}
}
