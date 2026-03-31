// Package worker contains background workers responsible
// for processing and exporting payments.
package worker

import (
	"context"
	"log/slog"
	"time"
)

type EtlPipline interface {
	Run(ctx context.Context) error
}

type Worker struct {
	etlPipline EtlPipline
	interval   time.Duration
	logger     *slog.Logger
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := w.etlPipline.Run(ctx); err != nil {
				w.logger.Error("etl pipline failed", "err", err)
			}
		}
	}
}

func NewWorker(etl EtlPipline, interval time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		etlPipline: etl,
		interval:   interval,
		logger:     logger,
	}
}
