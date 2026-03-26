package etl

import (
	"context"
)

type Producer[D any, ID comparable] interface {
	Fetch(ctx context.Context) ([]D, error)
	Ack(ctx context.Context, ids []ID) error
}

type Repository[D any, ID comparable] interface {
	SaveBatch(ctx context.Context, batch []D) error
	FetchForProcessing(ctx context.Context, limit int) ([]ID, []D, error)
	FetchProcessed(ctx context.Context, limit int) ([]ID, []D, error)
	MarkSent(ctx context.Context, ids []ID) error
	MarkFailed(ctx context.Context, ids []ID) error
}

type Consumer[D any] interface {
	InsertBatch(ctx context.Context, batch []D) error
}
