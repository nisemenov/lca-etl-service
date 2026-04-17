package repository

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
)

var hndlr = os.Stdout

// var hndlr := io.Discard

func NewTestSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	require.NoError(t, err)

	err = sqlite.Migrate(db)
	require.NoError(t, err)

	t.Cleanup(func() { _ = db.Close() })
	return db
}

func NewTestSQLitePaymentRepo(t *testing.T) *sqlitePaymentRepo {
	t.Helper()

	db := NewTestSQLiteDB(t)
	logger := slog.New(slog.NewTextHandler(hndlr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	repo := NewSQLitePaymentRepo(db, logger)
	return repo
}

func savePaymentBatch(ctx context.Context, repo *sqlitePaymentRepo, batch []domain.Payment) error {
	tx, err := repo.db.BeginTx(ctx, nil)
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
			p.Status,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func NewTestSQLiteYooPaymentRepo(t *testing.T) *sqliteYooPaymentRepo {
	t.Helper()

	db := NewTestSQLiteDB(t)
	logger := slog.New(slog.NewTextHandler(hndlr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	repo := NewSQLiteYooPaymentRepo(db, logger)
	return repo
}

func saveYooPaymentBatch(ctx context.Context, repo *sqliteYooPaymentRepo, batch []domain.YooPayment) error {
	tx, err := repo.db.BeginTx(ctx, nil)
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
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, yoo := range batch {
		_, err := stmt.ExecContext(ctx,
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
			yoo.Status,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
