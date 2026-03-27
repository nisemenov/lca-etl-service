package etl

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestETL_FetchAndSave_OK(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipline(producer, repo, consumer, logger)

	err := etl.FetchAndSave(context.Background())
	require.NoError(t, err)
	require.Equal(t, producer.batch, repo.batch)
}

func TestETL_Fetch_Error(t *testing.T) {
	producer := &mockProducer{err: errors.New("fetch failed")}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipline(producer, repo, consumer, logger)

	err := etl.FetchAndSave(context.Background())
	require.Error(t, err)
}

func TestETL_Save_Error(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{err: errors.New("save failed")}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipline(producer, repo, consumer, logger)

	err := etl.FetchAndSave(context.Background())
	require.Error(t, err)
}

func TestETL_ProcessAndSend_OK(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds: []int{1}}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipline(nil, repo, consumer, logger)

	err := etl.ProcessAndSend(context.Background())
	require.NoError(t, err)

	require.Equal(t, StatusSent, repo.etlStatus)
	require.Equal(t, repo.batch, consumer.insertedBatch)
}

func TestETL_ProcessAndSend_CH_Error(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds:[]int{1}}
	consumer := &mockConsumer{err: errors.New("CH error")}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipline(nil, repo, consumer, logger)

	err := etl.ProcessAndSend(context.Background())
	require.Error(t, err)
	require.Equal(t, StatusFailed, repo.etlStatus)
}

//
// func TestETL_Run_OK(t *testing.T) {
// 	producer := &mockProducer{
// 		data: []domain.Payment{
// 			{ID: 1},
// 			{ID: 2},
// 		},
// 	}
//
// 	repo := &mockRepo{
// 		toProcess: []domain.Payment{
// 			{ID: 1},
// 			{ID: 2},
// 		},
// 	}
//
// 	consumer := &mockConsumer{}
// 	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
//
// 	etl := etlPipline[domain.Payment, domain.PaymentID]{
// 		producer: producer,
// 		repo:     repo,
// 		consumer:   consumer,
// 		logger:   logger,
// 	}
//
// 	err := etl.Run(context.Background())
// 	require.NoError(t, err)
//
// 	require.Len(t, repo.saved, 2)
// 	require.Len(t, consumer.received, 2)
// }
