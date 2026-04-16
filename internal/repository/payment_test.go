package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/etl"
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

	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])
	require.Equal(t, etl.StatusNew, batch.Items[0].Status)
	require.NotNil(t, batch.Items[0].BatchID)
}

func TestPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Nil(t, batch)
}

func TestPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	savePaymentBatch(ctx, repo, []domain.Payment{{ID: 1, Status: etl.StatusNew}})

	batch, err := repo.FetchForProcessing(ctx)
	require.NoError(t, err)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])
	require.Len(t, batch.Items, 1)

	tx, _ := repo.db.BeginTx(ctx, nil)
	batch, err = repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
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

	savePaymentBatch(ctx, repo, []domain.Payment{{ID: 1, Status: etl.StatusNew}})

	err := repo.MarkStatus(ctx, []domain.PaymentID{1}, etl.StatusProcessing)
	require.NoError(t, err)

	tx, err := repo.db.BeginTx(ctx, nil)

	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Nil(t, batch)

	batch, err = repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
}

func TestPaymentRepo_MarkStatus_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.MarkStatus(ctx, []domain.PaymentID{}, etl.StatusProcessing)
	require.NoError(t, err)

	err = repo.MarkStatus(ctx, nil, etl.StatusProcessing)
	require.NoError(t, err)
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
	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusExported)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.PaymentID(2), batch.Items[0].ID)
}

func TestPyamentRepo_FetchStaleProcessingByBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	batchID := uuid.NewString()
	savePaymentBatch(ctx, repo, []domain.Payment{
		{ID: 1, Status: etl.StatusProcessing},
		{ID: 2, Status: etl.StatusProcessing, BatchID: &batchID},
		{ID: 3, Status: etl.StatusProcessing, BatchID: &batchID},
		{ID: 4, Status: etl.StatusProcessing, BatchID: new("uuid")},
	})
	tx, err := repo.db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, `
		UPDATE payments SET updated_at = datetime('now', ?)`,
		fmt.Sprintf("-%d seconds", int(staleProcessingTTL.Seconds())))
	tx.Commit()
	require.NoError(t, err)

	batch, err := repo.FetchStaleProcessingByBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch.Items, 2)
	require.Equal(t, batchID, *batch.Items[0].BatchID)
	require.Equal(t, batchID, *batch.Items[1].BatchID)
}
