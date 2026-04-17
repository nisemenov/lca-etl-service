// Package repository contains persistence interfaces and
// database-backed implementations for payment storage.
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/etl"
)

type sqliteYooPaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

// SaveBatch inserts a new batch of yoo payments with StatusNew.
// Ignores duplicates by primary key (id).
func (r *sqliteYooPaymentRepo) SaveBatch(ctx context.Context, batch []domain.YooPayment) error {
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
        INSERT INTO yookassa (
			id,
			case_id,
			debtor_id,
			full_name,
			credit_number,
			credit_issue_date,
			amount,
			yookassa_id,
			technical_status,
			yoo_created_at,
			execution_date_by_system,
			description,
			status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	inserted := int64(0)

	for _, yoo := range batch {
		res, err := stmt.ExecContext(ctx,
			yoo.ID,
			yoo.CaseID,
			yoo.DebtorID,
			yoo.FullName,
			yoo.CreditNumber,
			yoo.CreditIssueDate,
			yoo.Amount,
			yoo.YookassaID,
			yoo.TechnicalStatus,
			yoo.YooCreatedAt,
			yoo.ExecutionDateBySystem,
			yoo.Description,
			etl.StatusNew,
		)
		if err != nil {
			return fmt.Errorf("insert yookassa payment %d: %w", yoo.ID, err)
		}

		affected, _ := res.RowsAffected()
		inserted += affected
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	r.logger.Info("new yookassa payments batch saved successfully", "count", inserted)
	return nil
}

// FetchForProcessing selects payments with StatusNew,
// marks them as StatusProcessing in one atomic transaction,
// and returns them as *etl.Batch.
// Returns nil, nil if no records found.
//
// возвращает батч со status == StatusNew
func (r *sqliteYooPaymentRepo) FetchForProcessing(
	ctx context.Context,
) (*etl.Batch[domain.YooPaymentID, domain.YooPayment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	batch, err := r.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	if err != nil {
		return nil, fmt.Errorf("fetchYooPaymentsOnStatus failed: %w", err)
	}
	if batch == nil || len(batch.IDs) == 0 {
		r.logger.Info("empty batch for FetchForProcessing")
		return nil, nil
	}

	updCount, err := r.markStatusTx(ctx, tx, batch.IDs, etl.StatusProcessing)
	if err != nil {
		return nil, fmt.Errorf("mark StatusProcessing failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	r.logger.Info("payments fetched for processing successfully",
		"fetched count", len(batch.IDs),
		"updated count", updCount,
	)
	return batch, nil
}

// RequeueStaleProcessing finds payments stuck in PROCESSING state longer than TTL
// and requeues them back to NEW state for reprocessing.
func (r *sqliteYooPaymentRepo) RequeueStaleProcessing(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE yookassa
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
func (r *sqliteYooPaymentRepo) FetchByStatus(
	ctx context.Context,
	status etl.EtlStatus,
) (*etl.Batch[domain.YooPaymentID, domain.YooPayment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	batch, err := r.fetchYooPaymentsOnStatus(ctx, tx, status)
	if err != nil {
		return nil, err
	}

	if batch == nil {
		r.logger.Info("no payment instances found", "status", status)
		return nil, nil
	}

	return batch, nil
}

// MarkStatus updates status for given yoo payment IDs.
func (r *sqliteYooPaymentRepo) MarkStatus(ctx context.Context, ids []domain.YooPaymentID, status etl.EtlStatus) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := r.markStatusTx(ctx, tx, ids, status); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteExported removes exported yoo payments older than retention period.
func (r *sqliteYooPaymentRepo) DeleteExported(ctx context.Context) error {
	r.logger.Info("starting cleanup of exported yookassa payments")

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		DELETE FROM yookassa
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
		"exported yookassa payments deleted",
		"count", affected,
	)

	return nil
}

// markStatusTx updates status for given IDs within a transaction and
// returns count of updated statuses.
func (r *sqliteYooPaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.YooPaymentID,
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
        UPDATE yookassa
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

// fetchYooPaymentsOnStatus selects yoo payments by status within a transaction.
func (r *sqliteYooPaymentRepo) fetchYooPaymentsOnStatus(
	ctx context.Context,
	tx *sql.Tx,
	status etl.EtlStatus,
) (*etl.Batch[domain.YooPaymentID, domain.YooPayment], error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			id,
			case_id,
			debtor_id,
			full_name,
			credit_number,
			credit_issue_date,
			amount,
			yookassa_id,
			technical_status,
			yoo_created_at,
			execution_date_by_system,
			description,
			status,
			created_at,
			updated_at
        FROM yookassa
        WHERE status = ?
	`, status)
	if err != nil {
		return nil, fmt.Errorf("query payments with status %s: %w", status, err)
	}
	defer rows.Close()

	return scanYooPayments(rows)
}

// scanYooPayments scans SQL rows into a Batch.
// Returns nil if no rows found.
func scanYooPayments(rows *sql.Rows) (*etl.Batch[domain.YooPaymentID, domain.YooPayment], error) {
	var ids []domain.YooPaymentID
	var items []domain.YooPayment

	for rows.Next() {
		var yoo domain.YooPayment

		err := rows.Scan(
			&yoo.ID,
			&yoo.CaseID,
			&yoo.DebtorID,
			&yoo.FullName,
			&yoo.CreditNumber,
			&yoo.CreditIssueDate,
			&yoo.Amount,
			&yoo.YookassaID,
			&yoo.TechnicalStatus,
			&yoo.YooCreatedAt,
			&yoo.ExecutionDateBySystem,
			&yoo.Description,
			&yoo.Status,
			&yoo.CreatedAt,
			&yoo.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}

		ids = append(ids, yoo.ID)
		items = append(items, yoo)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	if len(items) == 0 {
		return nil, nil
	}

	return &etl.Batch[domain.YooPaymentID, domain.YooPayment]{IDs: ids, Items: items}, nil
}

func NewSQLiteYooPaymentRepo(db *sql.DB, logger *slog.Logger) *sqliteYooPaymentRepo {
	return &sqliteYooPaymentRepo{db: db, logger: logger}
}
