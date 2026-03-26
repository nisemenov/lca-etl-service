package repository

import (
	"context"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nisemenov/etl_service/internal/domain"
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

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusNew, 10)
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

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusNew, 10)
	require.NoError(t, err)
	require.Len(t, payments, 0)
}

// интеграционный, проверяет также markStatusTx
func TestPaymentRepo_FetchForProcessing(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{
		ID:                    1,
		CaseID:                1,
		DebtorID:              1,
		FullName:              "John Doe",
		CreditNumber:          "1",
		CreditIssueDate:       time.Now(),
		Amount:                domain.Money(1),
		DebtAmount:            domain.Money(1),
		ExecutionDateBySystem: time.Now(),
		Channel:               "sms",
		Status:                domain.StatusNew,
	}})

	_, payments, err := repo.FetchForProcessing(ctx, 10)
	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.NoError(t, payments[0].Validate())

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusProcessing, 10)
	require.NoError(t, err)
}

func TestPaymentRepo_FetchForProcessing_Atomic(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})

	_, batch1, _ := repo.FetchForProcessing(ctx, 10)
	_, batch2, _ := repo.FetchForProcessing(ctx, 10)

	require.Len(t, batch1, 1)
	require.Len(t, batch2, 0)
}

func TestPaymentRepo_MarkSent(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})
	repo.MarkSent(ctx, []domain.PaymentID{1})

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusNew, 10)
	require.NoError(t, err)
	require.Len(t, payments, 0)

	payments, err = repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusExported, 10)
	require.NoError(t, err)
	require.Len(t, payments, 1)
}

func TestPaymentRepo_MarkFailed(t *testing.T) {
	ctx := context.Background()
	repo := NewTestSQLitePaymentRepo(t)

	repo.SaveBatch(ctx, []domain.Payment{{ID: 1}})
	repo.MarkFailed(ctx, []domain.PaymentID{1})

	tx, err := repo.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	payments, err := repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusNew, 10)
	require.NoError(t, err)
	require.Len(t, payments, 0)

	payments, err = repo.fetchPaymentsOnStatus(ctx, tx, domain.StatusFailed, 10)
	require.NoError(t, err)
	require.Len(t, payments, 1)
}
