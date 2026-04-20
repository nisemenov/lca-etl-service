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

func TestYooPaymentRepo_SaveBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	instances := []domain.YooPayment{{ID: 1}}
	inserted, err := repo.SaveBatch(ctx, instances)
	require.NoError(t, err)
	require.Equal(t, len(instances), inserted)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	batch, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, domain.YooPaymentID(1), batch.IDs[0])
	require.Equal(t, etl.StatusNew, batch.Items[0].Status)
}

func TestYooPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	inserted, err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, inserted)

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	batch, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Nil(t, batch)
}

func TestYooPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	saveYooPaymentBatch(ctx, repo, []domain.YooPayment{{ID: 1, Status: etl.StatusNew}})

	batch, err := repo.FetchForProcessing(ctx)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.YooPaymentID(1), batch.IDs[0])

	tx, _ := repo.db.BeginTx(ctx, nil)
	batch, err = repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
}

func TestYooPaymentRepo_FetchByStatus(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	err := saveYooPaymentBatch(ctx, repo, []domain.YooPayment{{ID: 1, Status: etl.StatusSent}})
	require.NoError(t, err)

	batch, err := repo.FetchByStatus(ctx, etl.StatusSent)
	require.NoError(t, err)
	require.Len(t, batch.IDs, 1)
	require.Len(t, batch.Items, 1)
	require.Equal(t, domain.YooPaymentID(1), batch.IDs[0])
}

func TestYooPaymentRepo_MarkStatus(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	saveYooPaymentBatch(ctx, repo, []domain.YooPayment{{ID: 1, Status: etl.StatusNew}})

	err := repo.MarkStatus(ctx, []domain.YooPaymentID{1}, etl.StatusProcessing)
	require.NoError(t, err)

	tx, _ := repo.db.BeginTx(ctx, nil)

	batch, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.Nil(t, batch)

	batch, err = repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusProcessing)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
}

func TestYooPaymentRepo_MarkStatus_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	t.Run("empty ids slice", func(t *testing.T) {
		err := repo.MarkStatus(ctx, []domain.YooPaymentID{}, etl.StatusProcessing)
		require.NoError(t, err)
	})

	t.Run("ids == nil", func(t *testing.T) {
		err := repo.MarkStatus(ctx, nil, etl.StatusProcessing)
		require.NoError(t, err)
	})
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
	batch, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusExported)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Len(t, batch.IDs, 1)
	require.Equal(t, domain.YooPaymentID(2), batch.Items[0].ID)
}

func TestYooPaymentRepo_RequeueStaleProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLiteYooPaymentRepo(t)

	saveYooPaymentBatch(ctx, repo, []domain.YooPayment{
		{ID: 1, Status: etl.StatusProcessing},
		{ID: 2, Status: etl.StatusProcessing},
		{ID: 3, Status: etl.StatusProcessing},
	})

	tx, err := repo.db.BeginTx(ctx, nil)

	tx.ExecContext(ctx, `
		UPDATE yookassa
		SET updated_at = datetime('now', ?)
		WHERE id IN (1, 2)
	`, fmt.Sprintf("-%d seconds", int(staleProcessingTTL.Seconds())))
	tx.Commit()
	require.NoError(t, err)

	err = repo.RequeueStaleProcessing(ctx)
	require.NoError(t, err)

	tx, _ = repo.db.BeginTx(ctx, nil)
	batch, err := repo.fetchYooPaymentsOnStatus(ctx, tx, etl.StatusNew)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []domain.YooPaymentID{1, 2}, batch.IDs)
}
