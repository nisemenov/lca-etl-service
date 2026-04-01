// Package producer provides clients for fetching data
// from external producer services over HTTP.
package producer

import (
	"context"
	"log/slog"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
)

type paymentProducer struct {
	http   *httpclient.HTTPClient
	logger *slog.Logger
}

func (p *paymentProducer) Fetch(ctx context.Context) ([]domain.Payment, error) {
	var resp fetchPaymentsResponse

	err := p.http.Get(ctx, FetchPaymentsPath, &resp)
	if err != nil {
		p.logger.Error(
			"failed to fetch payment data",
			"err", err,
		)
		return nil, err
	}
	if len(resp.Data) == 0 {
		p.logger.Info("no new payments data to export")
		return nil, nil
	}

	// fill in output with validated data
	response := make([]domain.Payment, 0, len(resp.Data))
	for _, rawPayment := range resp.Data {
		if err := rawPayment.Validate(); err != nil {
			p.logger.Warn(
				"failed to validate raw payment data",
				"id", rawPayment.ID,
				"err", err,
			)
			continue
		}

		response = append(response, domain.Payment{
			ID:                    rawPayment.ID,
			CaseID:                rawPayment.CaseID,
			DebtorID:              rawPayment.DebtorID,
			FullName:              rawPayment.FullName,
			CreditNumber:          rawPayment.CreditNumber,
			CreditIssueDate:       rawPayment.CreditIssueDate,
			Amount:                domain.Money(rawPayment.Amount),
			DebtAmount:            domain.Money(rawPayment.DebtAmount),
			ExecutionDateBySystem: rawPayment.ExecutionDateBySystem,
			Channel:               rawPayment.Channel,
		})
	}
	if len(response) == 0 {
		p.logger.Warn("all payments invalid")
		return nil, err
	}

	p.logger.Info("payments fetched successfully", "count", len(response))
	return response, nil
}

func (p *paymentProducer) Acknowledge(ctx context.Context, ids []domain.PaymentID) error {
	if len(ids) == 0 {
		p.logger.Warn("empty payment ids batch for Acknowledge")
		return nil
	}

	err := p.http.Post(ctx, AckPaymentsPath, ackPaymentRequest{IDs: ids})
	if err != nil {
		p.logger.Error(
			"failed to insert payment ids into prod service",
			"err", err,
		)
		return err
	}
	return nil
}

func NewPaymentProducer(http *httpclient.HTTPClient, logger *slog.Logger) *paymentProducer {
	return &paymentProducer{http: http, logger: logger}
}
