package worker

import "context"

// JobFunc is pseudoworker with EtlPipeline interface
type JobFunc func(ctx context.Context) error

func (f JobFunc) Run(ctx context.Context) error {
	return f(ctx)
}
