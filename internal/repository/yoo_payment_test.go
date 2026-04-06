package repository

import (
	"context"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/etl"
	"github.com/stretchr/testify/require"
)

func TestYooPaymentRepo_SaveBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	err := repo.SaveBatch(ctx, []domain.YooPayment{{ID: 1}})
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.YooPaymentID(1), payments[0].ID)
	require.Equal(t, etl.StatusNew, payments[0].Status)
}

func TestYooPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 0)
}

func TestYooPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	err := repo.SaveBatch(ctx, []domain.YooPayment{{
		ID:                    1,
		CaseID:                1,
		DebtorID:              1,
		FullName:              "John Doe",
		CreditNumber:          "1",
		CreditIssueDate:       time.Now(),
		Amount:                domain.Money("1.0"),
		YookassaID:            "215d8da0-000f-50be-b000-0003308c89be",
		TechnicalStatus:       "succeeded",
		YooCreatedAt:          time.Now(),
		ExecutionDateBySystem: time.Now(),
		Description:           "sms",
	}})
	require.NoError(t, err)

	_, payments, err := repo.FetchForProcessing(ctx)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.NoError(t, payments[0].Validate())

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
}

func TestYooPaymentRepo_FetchSentIds(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	err := saveYooPaymentBatch(ctx, repo, []domain.YooPayment{{ID: 1, Status: etl.StatusSent}})
	require.NoError(t, err)

	ids, err := repo.FetchSentIds(ctx)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, domain.YooPaymentID(1), ids[0])
}

func TestYooPaymentRepo_FetchForProcessing_Atomic(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	repo.SaveBatch(ctx, []domain.YooPayment{{ID: 1}})

	_, batch1, _ := repo.FetchForProcessing(ctx)
	_, batch2, _ := repo.FetchForProcessing(ctx)

	require.Len(t, batch1, 1)
	require.Len(t, batch2, 0)
}

func TestYooPaymentRepo_MarkStatus(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)
	repo.SaveBatch(ctx, []domain.YooPayment{{ID: 1}})

	err := repo.MarkStatus(ctx, []domain.YooPaymentID{1}, etl.StatusProcessing)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 0)

	payments, err = repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, payments, 1)
}

func TestYooPaymentRepo_DeleteExported(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	saveYooPaymentBatch(ctx, repo, []domain.YooPayment{{ID: 1, Status: etl.StatusExported}, {ID: 2, Status: etl.StatusExported}})
	tx, _ := repo.db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, `UPDATE yookassa SET created_at = ? WHERE id = 1`, time.Now().AddDate(0, 0, -8))
	tx.Commit()

	err := repo.DeleteExported(ctx)
	require.NoError(t, err)

	tx, _ = repo.db.BeginTx(ctx, nil)
	payments, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusExported)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.YooPaymentID(2), payments[0].ID)
}
