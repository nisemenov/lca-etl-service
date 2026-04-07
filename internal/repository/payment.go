// Package repository contains persistence interfaces and
// database-backed implementations for payment storage.
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/etl"
)

type sqlitePaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

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
			status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var inserted int64
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

// FetchForProcessing меняет статусы в бд на StatusProcessing;
// возвращает батч со status == StatusNew, потому что в CH они не вставляются
func (r *sqlitePaymentRepo) FetchForProcessing(
	ctx context.Context,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}
	defer tx.Rollback()

	payments, err := r.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	if err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}

	if len(payments) == 0 {
		r.logger.Info("empty batch for FetchForProcessing")
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, nil
	}

	ids := make([]domain.PaymentID, 0, len(payments))
	for _, p := range payments {
		ids = append(ids, p.ID)
	}

	if err := r.markStatusTx(ctx, tx, ids, etl.StatusProcessing); err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}

	if err := tx.Commit(); err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}

	r.logger.Info("payments fetched for processing successfully", "count", len(ids))
	return &etl.Batch[domain.PaymentID, domain.Payment]{IDs: ids, Items: payments}, nil
}

func (r *sqlitePaymentRepo) FetchByStatus(
	ctx context.Context,
	status etl.EtlStatus,
) (*etl.Batch[domain.PaymentID, domain.Payment], error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}
	defer tx.Rollback()

	instances, err := r.fetchPaymentsOnStatus(ctx, tx, status)
	if err != nil {
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, err
	}

	if len(instances) == 0 {
		r.logger.Info(fmt.Sprintf("no payment instances with %s found", status))
		return &etl.Batch[domain.PaymentID, domain.Payment]{}, nil
	}

	ids := make([]domain.PaymentID, 0, len(instances))
	for _, p := range instances {
		ids = append(ids, p.ID)
	}
	return &etl.Batch[domain.PaymentID, domain.Payment]{IDs: ids, Items: instances}, nil
}

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

// DeleteExported deletes all StatusExported instances created earlier than 7 days
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

func (r *sqlitePaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.PaymentID,
	status etl.EtlStatus,
) error {
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

func (r *sqlitePaymentRepo) fetchPaymentsOnStatus(
	ctx context.Context,
	tx *sql.Tx,
	status etl.EtlStatus,
) ([]domain.Payment, error) {
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
		return nil, err
	}
	defer rows.Close()

	return scanPayments(rows)
}

func scanPayments(rows *sql.Rows) ([]domain.Payment, error) {
	var payments []domain.Payment
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
			return nil, err
		}
		payments = append(payments, p)
	}
	return payments, rows.Err()
}

func NewSQLitePaymentRepo(db *sql.DB, logger *slog.Logger) *sqlitePaymentRepo {
	return &sqlitePaymentRepo{db: db, logger: logger}
}
