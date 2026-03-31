// Package consumer contains implementations responsible for
// exporting processed data to external systems such as ClickHouse.
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
)

type clickHouseLoader struct {
	http   *httpclient.HTTPClient
	table  string
	logger *slog.Logger
}

func (c *clickHouseLoader) InsertBatch(ctx context.Context, batch []domain.Payment) error {
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

func (c *clickHouseLoader) insertOnce(ctx context.Context, batch []domain.Payment) error {
	if len(batch) == 0 {
		c.logger.Info("empty batch for InsertBatch")
		return nil
	}

	var buf bytes.Buffer

	for _, p := range batch {
		row, err := c.paymentToClickHouseRow(p)
		if err != nil {
			c.logger.Warn(
				"failed to build row",
				"payment.ID", p.ID,
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

func (c *clickHouseLoader) paymentToClickHouseRow(payment domain.Payment) ([]byte, error) {
	row := map[string]any{
		"case_id":                  payment.CaseID,
		"debtor_id":                payment.DebtorID,
		"full_name":                payment.FullName,
		"credit_number":            payment.CreditNumber,
		"credit_issue_date":        payment.CreditIssueDate.Format("2006-01-02 15:04:05.000"),
		"amount":                   payment.Amount,
		"debt_amount":              payment.DebtAmount,
		"execution_date_by_system": payment.ExecutionDateBySystem.Format("2006-01-02 15:04:05.000"),
		"channel":                  payment.Channel,
	}
	return json.Marshal(row)
}

func NewClickHouseLoader(http *httpclient.HTTPClient, table string, logger *slog.Logger) *clickHouseLoader {
	return &clickHouseLoader{http: http, table: table, logger: logger}
}
