// Package repository contains persistence interfaces and
// database-backed implementations for payment storage.
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/etl"
)

const staleProcessingTTL = 5 * time.Minute

type sqlitePaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

// SaveBatch inserts a batch of payments with StatusNew.
// Assigns a single batch_id to all inserted rows.
// Ignores duplicates by primary key (id).
func (r *sqlitePaymentRepo) SaveBatch(ctx context.Context, batch []domain.Payment) error {
	if len(batch) == 0 {
		r.logger.Info("empty batch for SaveBatch")
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
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
			status,
			batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	inserted := int64(0)
	batchID := uuid.NewString()

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
			batchID,
		)
		if err != nil {
			return fmt.Errorf("insert payment %d: %w", p.ID, err)
		}

		affected, _ := res.RowsAffected()
		inserted += affected
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	r.logger.Info("new payments batch saved successfully", "count", inserted)
	return nil
}

// FetchForProcessing selects payments with StatusNew,
// marks them as StatusProcessing, and returns them as a batch.
// Returns nil if no records found.
//
// возвращает батч со status == StatusNew, потому что в CH они не вставляются
func (r *sqlitePaymentRepo) FetchForProcessing(
	ctx context.Context,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	batch, err := r.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		r.logger.Info("empty batch for FetchForProcessing")
		return nil, nil
	}

	if err := r.markStatusTx(ctx, tx, batch.IDs, etl.StatusProcessing); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	r.logger.Info("payments fetched for processing successfully", "count", len(batch.IDs))
	return batch, nil
}

// FetchStaleProcessingByBatch selects one stale processing batch
// (based on updated_at TTL), refreshes its lease, and returns it.
// Returns nil if no stale batch found.
func (r *sqlitePaymentRepo) FetchStaleProcessingByBatch(
	ctx context.Context,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	var batchID string

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	err = tx.QueryRowContext(
		ctx,
		`
		SELECT batch_id
		FROM payments
		WHERE status = ?
		  AND batch_id IS NOT NULL
		  AND updated_at <= datetime('now', ?)
		GROUP BY batch_id
		ORDER BY MIN(updated_at)
		LIMIT 1
		`, etl.StatusProcessing, fmt.Sprintf("-%d seconds", int(staleProcessingTTL.Seconds())),
	).Scan(&batchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			r.logger.Info("no stale processing batch")
			return nil, nil
		}
		return nil, fmt.Errorf("select batch_id failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE payments
		SET updated_at = CURRENT_TIMESTAMP
		WHERE batch_id = ?
		  AND status = ?
	`, batchID, etl.StatusProcessing)
	if err != nil {
		return nil, fmt.Errorf("update updated_at failed: %w", err)
	}

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
			batch_id,
			created_at,
			updated_at
        FROM payments
		WHERE batch_id = ?
		  AND status = ?
		ORDER BY id
	`, batchID, etl.StatusProcessing)
	if err != nil {
		return nil, fmt.Errorf("select batch rows failed: %w", err)
	}
	defer rows.Close()

	batch, err := scanPayments(rows)
	if err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return batch, nil
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
		r.logger.Info("no payment instances found", "status", status)
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

	if err := r.markStatusTx(ctx, tx, ids, status); err != nil {
		return err
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
		"exported payments deleted",
		"count", affected,
	)

	return nil
}

// markStatusTx updates status for given IDs within a transaction.
func (r *sqlitePaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.PaymentID,
	status etl.EtlStatus,
) error {
	if len(ids) == 0 {
		return nil
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
        SET status = ?
        WHERE id IN (%s)
    `, placeholders)

	_, err := tx.ExecContext(ctx, query, args...)
	return err
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
			batch_id,
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
		var batchID sql.NullString

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
			&batchID,
			&p.CreatedAt,
			&p.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}

		if batchID.Valid {
			p.BatchID = new(batchID.String)
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
