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

func TestPaymentRepo_SaveBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.PaymentID(1), payments[0].ID)
	require.Equal(t, etl.StatusNew, payments[0].Status)
}

func TestPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 0)
}

func TestPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})

	batch, err := repo.FetchForProcessing(ctx)
	require.NoError(t, err)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])
	require.Len(t, batch.Items, 1)

	tx, _ := repo.db.BeginTx(ctx, nil)
	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, payments, 1)
}

func TestPaymentRepo_FetchByStatus(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := savePaymentBatch(ctx, repo, []domain.Payment{{ID: 1, Status: etl.StatusSent}})
	require.NoError(t, err)

	batch, err := repo.FetchByStatus(ctx, etl.StatusSent)
	require.NoError(t, err)
	require.Len(t, batch.IDs, 1)
	require.Len(t, batch.Items, 1)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])
}

func TestPaymentRepo_MarkStatus(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)
	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})

	err := repo.MarkStatus(ctx, []domain.PaymentID{1}, etl.StatusProcessing)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Len(t, payments, 0)

	payments, err = repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, payments, 1)
}

func TestPaymentRepo_DeleteExported(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	savePaymentBatch(ctx, repo, []domain.Payment{{ID: 1, Status: etl.StatusExported}, {ID: 2, Status: etl.StatusExported}})
	tx, _ := repo.db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, `UPDATE payments SET created_at = ? WHERE id = 1`, time.Now().AddDate(0, 0, -8))
	tx.Commit()

	err := repo.DeleteExported(ctx)
	require.NoError(t, err)

	tx, _ = repo.db.BeginTx(ctx, nil)
	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusExported)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.PaymentID(2), payments[0].ID)
}
