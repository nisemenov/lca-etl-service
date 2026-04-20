package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/etl"
	"github.com/stretchr/testify/require"
)

func TestPaymentRepo_SaveBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	instances := []domain.Payment{{ID: 1}}
	inserted, err := repo.SaveBatch(ctx, instances)
	require.NoError(t, err)
	require.Equal(t, len(instances), inserted)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])
	require.Equal(t, etl.StatusNew, batch.Items[0].Status)
}

func TestPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	inserted, err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, inserted)

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
	require.Len(t, batch.Items, 1)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.PaymentID(1), batch.IDs[0])

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

	tx, _ := repo.db.BeginTx(ctx, nil)

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

	t.Run("empty ids slice", func(t *testing.T) {
		err := repo.MarkStatus(ctx, []domain.PaymentID{}, etl.StatusProcessing)
		require.NoError(t, err)
	})

	t.Run("ids == nil", func(t *testing.T) {
		err := repo.MarkStatus(ctx, nil, etl.StatusProcessing)
		require.NoError(t, err)
	})
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

func TestPaymentRepo_RequeueStaleProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	savePaymentBatch(ctx, repo, []domain.Payment{
		{ID: 1, Status: etl.StatusProcessing},
		{ID: 2, Status: etl.StatusProcessing},
		{ID: 3, Status: etl.StatusProcessing},
	})

	tx, err := repo.db.BeginTx(ctx, nil)

	tx.ExecContext(ctx, `
		UPDATE payments
		SET updated_at = datetime('now', ?)
		WHERE id IN (1, 2)
	`, fmt.Sprintf("-%d seconds", int(staleProcessingTTL.Seconds())))
	tx.Commit()
	require.NoError(t, err)

	err = repo.RequeueStaleProcessing(ctx)
	require.NoError(t, err)

	tx, _ = repo.db.BeginTx(ctx, nil)
	batch, err := repo.fetchPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []domain.PaymentID{1, 2}, batch.IDs)
}
