// Package consumer contains implementations responsible for
// exporting processed data to external systems such as ClickHouse.
package consumer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
)

type domType interface {
	domain.Payment | domain.YooPayment
}

type rowBuilder[T domType] func(item T) ([]byte, error)

type clickHouseLoader[T domType] struct {
	http    *httpclient.HTTPClient
	table   string
	builder rowBuilder[T]
	logger  *slog.Logger
}

func (c *clickHouseLoader[T]) InsertBatch(ctx context.Context, batch []T) error {
	var lastErr error

	for i := range 3 {
		err := c.insertOnce(ctx, batch)
		if err == nil {
			return nil
		}

		lastErr = err

		c.logger.Warn("insert retry",
			"attempt", i+1,
			"err", err,
		)

		time.Sleep(time.Duration(i+1) * time.Second)
	}

	return lastErr
}

func (c *clickHouseLoader[T]) insertOnce(ctx context.Context, batch []T) error {
	if len(batch) == 0 {
		c.logger.Info("empty batch for InsertBatch")
		return nil
	}

	var buf bytes.Buffer

	for _, p := range batch {
		row, err := c.builder(p)
		if err != nil {
			c.logger.Warn(
				"failed to build row",
				"err", err,
			)
			continue
		}
		buf.Write(row)
		buf.WriteByte('\n')
	}
	query := fmt.Sprintf("/?query=INSERT+INTO+%s+FORMAT+JSONEachRow", c.table)

	err := c.http.PostRaw(ctx, query, "application/json", &buf)
	if err != nil {
		c.logger.Error(
			"InsertBatch failed",
			"table", c.table,
			"batch_size", len(batch),
			"err", err,
		)
		return fmt.Errorf("InsertBatch failed: %w", err)
	}

	c.logger.Info("batch inserted successfully", "count", len(batch))
	return nil
}

func NewClickHouseLoader[T domType](
	http *httpclient.HTTPClient, table string, builder rowBuilder[T], logger *slog.Logger,
) *clickHouseLoader[T] {
	return &clickHouseLoader[T]{http: http, table: table, builder: builder, logger: logger}
}
