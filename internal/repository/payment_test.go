package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestPaymentRepo_SaveBatch(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})
	require.NoError(t, err)

	payments, err := repo.fetchNewPayments(ctx, 10)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.PaymentID(1), payments[0].ID)
	require.Equal(t, domain.StatusNew, payments[0].Status)
}

func TestPaymentRepo_SaveBatch_Empty(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	err := repo.SaveBatch(ctx, nil)
	require.NoError(t, err)

	payments, err := repo.fetchNewPayments(ctx, 10)
	require.NoError(t, err)
	require.Len(t, payments, 0)
}

// интеграционный, проверяет также markStatusTx
func TestPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})

	payments, err := repo.FetchForProcessing(ctx, 10)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.StatusProcessing, payments[0].Status)
}

func TestPaymentRepo_FetchForProcessing_Atomic(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})

	batch1, _ := repo.FetchForProcessing(ctx, 10)
	batch2, _ := repo.FetchForProcessing(ctx, 10)

	require.Len(t, batch1, 1)
	require.Len(t, batch2, 0)
}

func TestPaymentRepo_MarkSent(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})
	repo.MarkSent(ctx, []domain.PaymentID{1})

	payments, err := repo.fetchNewPayments(ctx, 10)
	require.NoError(t, err)
	require.Len(t, payments, 0)
}
