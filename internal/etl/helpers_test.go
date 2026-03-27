package etl

import (
	"context"
)

type mockProducer struct {
	batch []string
	err   error
}

func (p *mockProducer) Fetch(ctx context.Context) ([]string, error) {
	return p.batch, p.err
}

func (p *mockProducer) Ack(ctx context.Context, ids []int) error {
	return p.err
}

type mockRepo struct {
	batch     []string
	newIds    []int
	etlStatus EtlStatus
	err       error
}

func (r *mockRepo) SaveBatch(ctx context.Context, batch []string) error {
	r.batch = batch
	return r.err
}

func (r *mockRepo) FetchForProcessing(ctx context.Context, limit int) ([]int, []string, error) {
	return r.newIds, r.batch, r.err
}

func (r *mockRepo) FetchProcessed(ctx context.Context, limit int) ([]int, []string, error) {
	return nil, nil, nil
}

func (r *mockRepo) MarkStatus(ctx context.Context, ids []int, status EtlStatus) error {
	r.etlStatus = status
	return r.err
}

type mockConsumer struct {
	insertedBatch []string
	err           error
}

func (c *mockConsumer) InsertBatch(ctx context.Context, batch []string) error {
	if c.err == nil {
		c.insertedBatch = batch
	}
	return c.err
}
