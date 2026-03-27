// Package worker contains background workers responsible
// for processing and exporting payments.
package worker

import (
	"context"
	"log/slog"
	"time"
)

type Runner interface {
	Run(ctx context.Context) error
}

type Worker struct {
	runner   Runner
	interval time.Duration
	logger   *slog.Logger
}

func New(r Runner, interval time.Duration, l *slog.Logger) *Worker {
	return &Worker{
		runner:   r,
		interval: interval,
		logger:   l,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := w.runner.Run(ctx); err != nil {
				w.logger.Error("etl failed", "err", err)
			}
		}
	}
}
