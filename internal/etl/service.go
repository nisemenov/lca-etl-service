// Package etl coordinates the end-to-end ETL workflow,
// orchestrating producers, repositories, and workers.
package etl

import (
	"context"
	"fmt"
	"log/slog"
)

type etlPipline[D any, ID comparable] struct {
	producer Producer[D, ID]
	repo     Repository[D, ID]
	consumer Consumer[D]
	logger   *slog.Logger
}

func (etl *etlPipline[D, ID]) FetchAndSave(ctx context.Context) error {
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

func (etl *etlPipline[D, ID]) ProcessAndSend(ctx context.Context) error {
	ids, instances, err := etl.repo.FetchForProcessing(ctx, 500)
	if err != nil {
		return fmt.Errorf("repository fetch failed: %w", err)
	}

	if len(instances) == 0 {
		etl.logger.Warn("no instances for processing")
		return nil
	}

	if err := etl.consumer.InsertBatch(ctx, instances); err != nil {
		if err2 := etl.repo.MarkStatus(ctx, ids, StatusFailed); err2 != nil {
			etl.logger.Error("failed to mark instances as failed", "err", err2)
		} else {
			etl.logger.Warn("batch marked as failed", "ids", ids)
		}
		return fmt.Errorf("clickhouse insert failed: %w", err)
	}

	if err := etl.repo.MarkStatus(ctx, ids, StatusSent); err != nil {
		return fmt.Errorf("mark sent failed: %w", err)
	}

	etl.logger.Info("instances inserted into clickhouse", "count", len(instances))
	return nil
}

func (etl *etlPipline[D, ID]) Acknowledge(ctx context.Context) error {
	// ids, _, err := etl.repo.FetchForProcessing(ctx, 100)
	// if err != nil {
	// 	return err
	// }
	//
	// if len(data) == 0 {
	// 	return nil
	// }
	//
	// var ids []ID
	// for _, d := range data {
	// 	ids = append(ids, d.ID)
	// }
	//
	// return etl.repo.MarkStatus(ctx, ids)
	return nil
}

// func (etl *etlPipline[D, ID]) Run(ctx context.Context) error {
// 	if err := etl.FetchAndSave(ctx); err != nil {
// 		p.logger.Error("fetch stage failed", "err", err)
// 		return err
// 	}
//
// 	if err := etl.ProcessAndSend(ctx); err != nil {
// 		p.logger.Error("process stage failed", "err", err)
// 		return err
// 	}
//
// 	if err := etl.Acknowledge(ctx); err != nil {
// 		p.logger.Error("ack stage failed", "err", err)
// 		return err
// 	}
//
// 	return nil
// }

func NewETLPipline[D any, ID comparable](
	producer Producer[D, ID],
	repo Repository[D, ID],
	consumer Consumer[D],
	logger *slog.Logger,
) *etlPipline[D, ID] {
	return &etlPipline[D, ID]{producer: producer, repo: repo, consumer: consumer, logger: logger}
}
