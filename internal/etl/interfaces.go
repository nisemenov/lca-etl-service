package etl

import (
	"context"
)

type Batch[ID comparable, D any] struct {
	IDs   []ID
	Items []D
}

type Producer[ID comparable, D any] interface {
	Fetch(ctx context.Context) ([]D, error)
	Acknowledge(ctx context.Context, ids []ID) error
}

type Repository[ID comparable, D any] interface {
	SaveBatch(ctx context.Context, batch []D) error
	FetchForProcessing(ctx context.Context) (*Batch[ID, D], error)
	FetchByStatus(ctx context.Context, status EtlStatus) (*Batch[ID, D], error)
	MarkStatus(ctx context.Context, ids []ID, status EtlStatus) error
	DeleteExported(ctx context.Context) error
}

type Consumer[D any] interface {
	InsertBatch(ctx context.Context, batch []D) error
}
