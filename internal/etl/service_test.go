package etl

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestETL_Fetch_OK(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	fetched, inserted, err := etl.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(producer.batch), fetched)
	require.Equal(t, len(producer.batch), inserted)
}

func TestETL_Fetch_Error(t *testing.T) {
	producer := &mockProducer{err: errors.New("fetch failed")}
	repo := &mockRepo{}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	fetched, inserted, err := etl.fetch(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, fetched)
	require.Equal(t, 0, inserted)
}

func TestETL_Save_Error(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{err: errors.New("save failed")}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	fetched, inserted, err := etl.fetch(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, fetched)
	require.Equal(t, 0, inserted)
}

func TestETL_Process_OK(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds: []int{1}}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(nil, repo, consumer, logger)

	processed, err := etl.process(context.Background())
	require.NoError(t, err)

	require.Equal(t, StatusSent, repo.etlStatus)
	require.Equal(t, len(repo.batch), processed)
}

func TestETL_Process_CH_Error(t *testing.T) {
	repo := &mockRepo{batch: []string{"test_batch"}, newIds: []int{1}, etlStatus: StatusNew}
	consumer := &mockConsumer{err: errors.New("CH error")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(nil, repo, consumer, logger)

	processed, err := etl.process(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, processed)
}

func TestAcknowledge_OK(t *testing.T) {
	ctx := context.Background()
	producer := &mockProducer{}
	repo := &mockRepo{sentIds: []int{1}, etlStatus: StatusSent}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, nil, logger)

	ack, err := etl.acknowledge(ctx)

	require.NoError(t, err)
	require.Equal(t, repo.sentIds, producer.ackIds)
	require.Equal(t, StatusExported, repo.etlStatus)
	require.Equal(t, len(repo.sentIds), ack)
}

func TestAcknowledge_EmptyBatch(t *testing.T) {
	producer := &mockProducer{}
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	etl := NewETLPipeline(producer, repo, nil, logger)

	ack, err := etl.acknowledge(context.Background())

	require.NoError(t, err)
	require.Equal(t, 0, ack)
}

func TestETL_Run_OK(t *testing.T) {
	producer := &mockProducer{batch: []string{"test_batch"}}
	repo := &mockRepo{newIds: []int{1}, sentIds: []int{1}}
	consumer := &mockConsumer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	etl := NewETLPipeline(producer, repo, consumer, logger)

	err := etl.Run(context.Background())
	require.NoError(t, err)
}
