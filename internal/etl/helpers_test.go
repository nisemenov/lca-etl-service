package etl

import (
	"context"
)

type mockProducer struct {
	batch  []string
	ackIds []int
	err    error
}

func (p *mockProducer) Fetch(ctx context.Context) ([]string, error) {
	return p.batch, p.err
}

func (p *mockProducer) Acknowledge(ctx context.Context, ids []int) error {
	p.ackIds = ids
	return p.err
}

type mockRepo struct {
	batch     []string
	newIds    []int
	sentIds   []int
	etlStatus EtlStatus
	err       error
}

func (r *mockRepo) SaveBatch(ctx context.Context, batch []string) error {
	r.batch = batch
	return r.err
}

func (r *mockRepo) FetchForProcessing(ctx context.Context) (*Batch[int, string], error) {
	return &Batch[int, string]{r.newIds, r.batch}, r.err
}

func (r *mockRepo) FetchByStatus(ctx context.Context, status EtlStatus) (*Batch[int, string], error) {
	return &Batch[int, string]{IDs: r.sentIds, Items: nil}, nil
}

func (r *mockRepo) MarkStatus(ctx context.Context, ids []int, status EtlStatus) error {
	r.etlStatus = status
	return r.err
}

func (r *mockRepo) DeleteExported(ctx context.Context) error { return nil }

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
