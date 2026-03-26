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
)

type sqlitePaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

func (r *sqlitePaymentRepo) SaveBatch(ctx context.Context, batch []domain.Payment) error {
	if len(batch) == 0 {
		r.logger.Warn("empty payments batch for SaveBatch")

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
        ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range batch {
		_, err := stmt.ExecContext(ctx,
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
			domain.StatusNew,
		)
		if err != nil {
			return fmt.Errorf("insert payment %d: %w", p.ID, err)
		}
	}

	return tx.Commit()
}

// FetchForProcessing меняет статусы в бд на StatusProcessing;
// возвращает батч со status == StatusNew, потому что в CH они не вставляются
func (r *sqlitePaymentRepo) FetchForProcessing(ctx context.Context, limit int) ([]domain.PaymentID, []domain.Payment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	payments, err := r.fetchPaymentsOnStatus(ctx, tx, domain.StatusNew, limit)
	if err != nil {
		return nil, nil, err
	}

	if len(payments) == 0 {
		r.logger.Warn("empty payments batch for FetchForProcessing")
		return nil, nil, nil
	}

	ids := make([]domain.PaymentID, 0, len(payments))
	for _, p := range payments {
		ids = append(ids, p.ID)
	}

	if err := r.markStatusTx(ctx, tx, ids, domain.StatusProcessing); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}

	return ids, payments, nil
}

// retry logic
func (r *sqlitePaymentRepo) FetchProcessed(
	ctx context.Context,
	limit int,
) ([]domain.PaymentID, []domain.Payment, error) {
	return nil, nil, nil
}

func (r *sqlitePaymentRepo) MarkSent(ctx context.Context, ids []domain.PaymentID) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := r.markStatusTx(ctx, tx, ids, domain.StatusExported); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *sqlitePaymentRepo) MarkFailed(ctx context.Context, ids []domain.PaymentID) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := r.markStatusTx(ctx, tx, ids, domain.StatusFailed); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *sqlitePaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.PaymentID,
	status domain.PaymentStatus,
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
	status domain.PaymentStatus,
	limit int,
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
        ORDER BY created_at ASC
        LIMIT ?
	`, status, limit)
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
