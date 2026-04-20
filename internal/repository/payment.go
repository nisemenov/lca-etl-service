// Package repository contains persistence interfaces and
// database-backed implementations for payment storage.
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/etl"
)

const staleProcessingTTL = 15 * time.Minute

type sqlitePaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

// SaveBatch inserts a new batch of payments with StatusNew.
// Ignores duplicates by primary key (id).
func (r *sqlitePaymentRepo) SaveBatch(ctx context.Context, batch []domain.Payment) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO payments (
            id,
            case_id,
            debtor_id,
            full_name,
            credit_number,
            credit_issue_date,
            amount,
            debt_amount,
            execution_date_by_system,
            channel,
            status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
    `)
	if err != nil {
		return 0, fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	inserted := 0

	for _, p := range batch {
		res, err := stmt.ExecContext(ctx,
			p.ID,
			p.CaseID,
			p.DebtorID,
			p.FullName,
			p.CreditNumber,
			p.CreditIssueDate,
			p.Amount,
			p.DebtAmount,
			p.ExecutionDateBySystem,
			p.Channel,
			etl.StatusNew,
		)
		if err != nil {
			return 0, fmt.Errorf("insert payment %d: %w", p.ID, err)
		}

		affected, _ := res.RowsAffected()
		inserted += int(affected)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}

	return inserted, nil
}

// FetchForProcessing selects payments with StatusNew,
// marks them as StatusProcessing in one atomic transaction,
// and returns them as *etl.Batch.
// Returns nil, nil if no records found.
//
// возвращает батч со status == StatusNew
func (r *sqlitePaymentRepo) FetchForProcessing(
	ctx context.Context,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	batch, err := r.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	if err != nil {
		return nil, fmt.Errorf("fetchPaymentsOnStatus failed: %w", err)
	}
	if batch == nil || len(batch.IDs) == 0 {
		return nil, nil
	}

	updCount := 0
	for _, b := range chunkItems(batch.IDs, SQLParamsLimit) {
		count, err := r.markStatusTx(ctx, tx, b, etl.StatusProcessing)
		if err != nil {
			return nil, fmt.Errorf("mark StatusProcessing failed: %w", err)
		}
		updCount += count
	}
	if updCount != len(batch.IDs) {
		return nil, fmt.Errorf(
			"partial update: expected=%d, updated=%d",
			len(batch.IDs), updCount,
		)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return batch, nil
}

// RequeueStaleProcessing finds payments stuck in PROCESSING state longer than TTL
// and requeues them back to NEW state for reprocessing.
func (r *sqlitePaymentRepo) RequeueStaleProcessing(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE payments
		SET status = ?,
		    updated_at = CURRENT_TIMESTAMP
		WHERE status = ?
		  AND updated_at <= datetime('now', ?)
	`,
		etl.StatusNew,
		etl.StatusProcessing,
		fmt.Sprintf("-%d seconds", int(staleProcessingTTL.Seconds())),
	)
	if err != nil {
		return fmt.Errorf("update stale processing payments failed: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	r.logger.Info(
		"stale processing payments requeued",
		"count", affected,
	)

	return nil
}

// FetchByStatus returns all payments with given status as a batch.
// Returns nil if no records found.
func (r *sqlitePaymentRepo) FetchByStatus(
	ctx context.Context,
	status etl.EtlStatus,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	batch, err := r.fetchPaymentsOnStatus(ctx, tx, status)
	if err != nil {
		return nil, err
	}

	if batch == nil {
		return nil, nil
	}

	return batch, nil
}

// MarkStatus updates status for given payment IDs.
func (r *sqlitePaymentRepo) MarkStatus(ctx context.Context, ids []domain.PaymentID, status etl.EtlStatus) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, b := range chunkItems(ids, SQLParamsLimit) {
		if _, err := r.markStatusTx(ctx, tx, b, status); err != nil {
			return fmt.Errorf("mark %s failed: %w", status, err)
		}
	}
	return tx.Commit()
}

// DeleteExported removes exported payments older than retention period.
func (r *sqlitePaymentRepo) DeleteExported(ctx context.Context) error {
	r.logger.Info("starting cleanup of exported payments")

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		DELETE FROM payments
		WHERE status = ?
		AND created_at < datetime('now', '-7 days')
	`, etl.StatusExported)
	if err != nil {
		return err
	}

	affected, _ := res.RowsAffected()

	if err = tx.Commit(); err != nil {
		return err
	}

	r.logger.Info(
		"exported instances deleted",
		"count", affected,
	)

	return nil
}

// markStatusTx updates status for given IDs within a transaction and
// returns count of updated statuses.
func (r *sqlitePaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.PaymentID,
	status etl.EtlStatus,
) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]any, 0, len(ids)+1)
	args = append(args, status)
	for _, id := range ids {
		args = append(args, id)
	}

	query := fmt.Sprintf(`
        UPDATE payments
        SET status = ?,
			updated_at = CURRENT_TIMESTAMP
        WHERE id IN (%s)
    `, placeholders)

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("update payment statuses failed: %w", err)
	} else {
		affected, _ := res.RowsAffected()
		return int(affected), nil
	}
}

// fetchPaymentsOnStatus selects payments by status within a transaction.
func (r *sqlitePaymentRepo) fetchPaymentsOnStatus(
	ctx context.Context,
	tx *sql.Tx,
	status etl.EtlStatus,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			id,
			case_id,
			debtor_id,
			full_name,
			credit_number,
			credit_issue_date,
			amount,
			debt_amount,
			execution_date_by_system,
			channel,
			status,
			created_at,
			updated_at
		FROM payments
		WHERE status = ?
	`, status)
	if err != nil {
		return nil, fmt.Errorf("query payments with status %s: %w", status, err)
	}
	defer rows.Close()

	return scanPayments(rows)
}

// scanPayments scans SQL rows into a Batch.
// Returns nil if no rows found.
func scanPayments(rows *sql.Rows) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	var ids []domain.PaymentID
	var items []domain.Payment

	for rows.Next() {
		var p domain.Payment

		err := rows.Scan(
			&p.ID,
			&p.CaseID,
			&p.DebtorID,
			&p.FullName,
			&p.CreditNumber,
			&p.CreditIssueDate,
			&p.Amount,
			&p.DebtAmount,
			&p.ExecutionDateBySystem,
			&p.Channel,
			&p.Status,
			&p.CreatedAt,
			&p.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}

		ids = append(ids, p.ID)
		items = append(items, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	if len(items) == 0 {
		return nil, nil
	}

	return &etl.Batch[domain.PaymentID, domain.Payment]{IDs: ids, Items: items}, nil
}

// NewSQLitePaymentRepo creates a new sqlitePaymentRepo instance.
func NewSQLitePaymentRepo(db *sql.DB, logger *slog.Logger) *sqlitePaymentRepo {
	return &sqlitePaymentRepo{db: db, logger: logger}
}
