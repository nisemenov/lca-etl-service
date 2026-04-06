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

type sqliteYooPaymentRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

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

	var inserted int64
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

// FetchForProcessing меняет статусы в бд на StatusProcessing;
// возвращает батч со status == StatusNew, потому что в CH они не вставляются
func (r *sqliteYooPaymentRepo) FetchForProcessing(
	ctx context.Context,
) ([]domain.YooPaymentID, []domain.YooPayment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	payments, err := r.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	if err != nil {
		return nil, nil, err
	}

	if len(payments) == 0 {
		r.logger.Info("empty batch for FetchForProcessing")
		return nil, nil, nil
	}

	ids := make([]domain.YooPaymentID, 0, len(payments))
	for _, p := range payments {
		ids = append(ids, p.ID)
	}

	if err := r.markStatusTx(ctx, tx, ids, etl.StatusProcessing); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}

	r.logger.Info("yookassa payments fetched for processing successfully", "count", len(ids))
	return ids, payments, nil
}

func (r *sqliteYooPaymentRepo) FetchSentIds(ctx context.Context) ([]domain.YooPaymentID, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	payments, err := r.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusSent)
	if err != nil {
		return nil, err
	}

	if len(payments) == 0 {
		r.logger.Info("empty batch for FetchSentIds")
		return nil, nil
	}

	ids := make([]domain.YooPaymentID, 0, len(payments))
	for _, p := range payments {
		ids = append(ids, p.ID)
	}
	return ids, nil
}

// retry logic
func (r *sqliteYooPaymentRepo) FetchProcessed(
	ctx context.Context,
) ([]domain.YooPaymentID, []domain.YooPayment, error) {
	return nil, nil, nil
}

func (r *sqliteYooPaymentRepo) MarkStatus(ctx context.Context, ids []domain.YooPaymentID, status etl.EtlStatus) error {
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

func (r *sqliteYooPaymentRepo) markStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	ids []domain.YooPaymentID,
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
        UPDATE yookassa
        SET status = ?
        WHERE id IN (%s)
    `, placeholders)

	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func (r *sqliteYooPaymentRepo) fetchYooPaymentsOnStatus(
	ctx context.Context,
	tx *sql.Tx,
	status etl.EtlStatus,
) ([]domain.YooPayment, error) {
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
		return nil, err
	}
	defer rows.Close()

	return scanYooPayments(rows)
}

func scanYooPayments(rows *sql.Rows) ([]domain.YooPayment, error) {
	var yooPayments []domain.YooPayment
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
			return nil, err
		}
		yooPayments = append(yooPayments, yoo)
	}
	return yooPayments, rows.Err()
}

func NewSQLiteYooPaymentRepo(db *sql.DB, logger *slog.Logger) *sqliteYooPaymentRepo {
	return &sqliteYooPaymentRepo{db: db, logger: logger}
}
