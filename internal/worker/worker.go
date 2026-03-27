// Package worker contains background workers responsible
// for processing and exporting payments.
package worker

// func worker(
// 	ctx context.Context,
// 	jobs <-chan domain.Payment,
// 	repo repository.PaymentRepository,
// 	loader consumer.ClickHouseLoader,
// ) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case p, ok := <-jobs:
// 			if !ok {
// 				return
// 			}
//
// 			if err := loader.Insert(ctx, p); err != nil {
// 				// лог + retry позже
// 				continue
// 			}
//
// 			repo.MarkSent(ctx, []domain.PaymentID{p.ID})
// 		}
// 	}
// }
