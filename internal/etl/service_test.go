package etl

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestETL_Fetch_OK(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	err := etl.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, producer.batch, repo.batch)
}

func TestETL_Fetch_Error(t *testing.T) {
	producer := &mockProducer{err: errors.New("fetch failed")}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	err := etl.fetch(context.Background())
	require.Error(t, err)
}

func TestETL_Save_Error(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{err: errors.New("save failed")}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	err := etl.fetch(context.Background())
	require.Error(t, err)
}

func TestETL_Process_OK(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds: []int{1}}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(nil, repo, consumer, logger)

	err := etl.process(context.Background())
	require.NoError(t, err)

	require.Equal(t, StatusSent, repo.etlStatus)
	require.Equal(t, repo.batch, consumer.insertedBatch)
}

func TestETL_Process_CH_Error(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds: []int{1}}
	consumer := &mockConsumer{err: errors.New("CH error")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(nil, repo, consumer, logger)

	err := etl.process(context.Background())
	require.Error(t, err)
	require.Equal(t, StatusFailed, repo.etlStatus)
}

func TestAcknowledge_OK(t *testing.T) {
	ctx := context.Background()
	producer := &mockProducer{}
	repo := &mockRepo{sentIds: []int{1}, etlStatus: StatusSent}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, nil, logger)

	err := etl.acknowledge(ctx)

	require.NoError(t, err)
	require.Equal(t, repo.sentIds, producer.ackIds)
	require.Equal(t, StatusExported, repo.etlStatus)
}

func TestAcknowledge_NoRecords(t *testing.T) {
	producer := &mockProducer{}
	repo := &mockRepo{etlStatus: StatusSent}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, nil, logger)

	err := etl.acknowledge(context.Background())

	require.NoError(t, err)
	require.Nil(t, producer.ackIds)
	require.Equal(t, StatusSent, repo.etlStatus)
}

func TestETL_Run_OK(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{newIds: []int{1}, sentIds: []int{1}}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	err := etl.Run(context.Background())
	require.NoError(t, err)

	//fetch prt
	require.Equal(t, producer.batch, repo.batch)
	//process prt
	require.Equal(t, producer.batch, consumer.insertedBatch)
	//ack prt
	require.Equal(t, StatusExported, repo.etlStatus)
	require.Equal(t, repo.newIds, producer.ackIds)
}
