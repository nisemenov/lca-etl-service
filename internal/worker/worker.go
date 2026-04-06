// Package worker contains background workers responsible
// for processing and exporting payments.
package worker

import (
	"context"
	"log/slog"
	"time"
)

// EtlPipeline is the common interface for Worker.etlPipeline and JobFunc
type EtlPipeline interface {
	Run(ctx context.Context) error
}

type Worker struct {
	etlPipeline EtlPipeline
	interval    time.Duration
	logger      *slog.Logger
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := w.etlPipeline.Run(ctx); err != nil {
				w.logger.Error("etl pipeline failed", "err", err)
			}
		}
	}
}

func NewWorker(etl EtlPipeline, interval time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		etlPipeline: etl,
		interval:    interval,
		logger:      logger,
	}
}
