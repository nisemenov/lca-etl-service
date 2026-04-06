package etl

import (
	"context"
)

type Producer[D any, ID comparable] interface {
	Fetch(ctx context.Context) ([]D, error)
	Acknowledge(ctx context.Context, ids []ID) error
}

type Repository[D any, ID comparable] interface {
	SaveBatch(ctx context.Context, batch []D) error
	FetchForProcessing(ctx context.Context) ([]ID, []D, error)
	FetchProcessed(ctx context.Context) ([]ID, []D, error)
	FetchSentIds(ctx context.Context) ([]ID, error)
	MarkStatus(ctx context.Context, ids []ID, status EtlStatus) error
}

type Consumer[D any] interface {
	InsertBatch(ctx context.Context, batch []D) error
}
