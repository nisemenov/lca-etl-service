// Package consumer contains implementations responsible for
// exporting processed data to external systems such as ClickHouse.
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
)

type clickHouseLoader struct {
	http   *httpclient.HTTPClient
	table  string
	logger *slog.Logger
}

func (c *clickHouseLoader) InsertBatch(ctx context.Context, payments []domain.Payment) error {
	if len(payments) == 0 {
		c.logger.Warn("empty payments batch for CH InsertBatch")
		return nil
	}

	var buf bytes.Buffer

	for _, p := range payments {
		row, err := c.paymentToClickHouseRow(p)
		if err != nil {
			c.logger.Warn(
				"failed to build clickhouse row",
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
			"CH InsertBatch failed",
			"table", c.table,
			"batch_size", len(payments),
			"err", err,
		)
		return fmt.Errorf("CH InsertBatch failed: %w", err)
	}
	return nil
}

func (c *clickHouseLoader) paymentToClickHouseRow(payment domain.Payment) ([]byte, error) {
	row := map[string]any{
		"id":                       payment.ID,
		"case_id":                  payment.CaseID,
		"debtor_id":                payment.DebtorID,
		"full_name":                payment.FullName,
		"credit_number":            payment.CreditNumber,
		"credit_issue_date":        payment.CreditIssueDate,
		"amount":                   payment.Amount.Float64(),
		"debt_amount":              payment.DebtAmount.Float64(),
		"execution_date_by_system": payment.ExecutionDateBySystem,
		"channel":                  payment.Channel,
		"status":                   payment.Status,
		"created_at":               payment.CreatedAt,
		"updated_at":               payment.UpdatedAt,
	}
	return json.Marshal(row)
}

func NewClickHouseLoader(http *httpclient.HTTPClient, table string, logger *slog.Logger) *clickHouseLoader {
	return &clickHouseLoader{http: http, table: table, logger: logger}
}
