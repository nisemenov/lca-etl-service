package repository

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
)

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
	db := NewTestSQLiteDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

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
