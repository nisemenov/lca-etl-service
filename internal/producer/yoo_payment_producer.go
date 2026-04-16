// Package producer provides clients for fetching data
// from external producer services over HTTP.
package producer

import (
	"context"
	"log/slog"

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/httpclient"
)

type yooPaymentProducer struct {
	http   *httpclient.HTTPClient
	logger *slog.Logger
}

func (y *yooPaymentProducer) Fetch(ctx context.Context) ([]domain.YooPayment, error) {
	var resp fetchYooPaymentsResponse

	err := y.http.Get(ctx, YookassaPath, &resp)
	if err != nil {
		y.logger.Error(
			"failed to fetch yookassa payment data",
			"err", err,
		)
		return []domain.YooPayment{}, err
	}
	if len(resp.Data) == 0 {
		y.logger.Info("no new yookassa payments data to export")
		return []domain.YooPayment{}, nil
	}

	// fill in output with validated data
	response := make([]domain.YooPayment, 0, len(resp.Data))
	for _, rawYooPayment := range resp.Data {
		if err := rawYooPayment.Validate(); err != nil {
			y.logger.Warn(
				"failed to validate raw yookassa payment data",
				"id", rawYooPayment.ID,
				"err", err,
			)
			continue
		}

		response = append(response, domain.YooPayment{
			ID:                    rawYooPayment.ID,
			CaseID:                rawYooPayment.CaseID,
			DebtorID:              rawYooPayment.DebtorID,
			FullName:              rawYooPayment.FullName,
			CreditNumber:          rawYooPayment.CreditNumber,
			CreditIssueDate:       rawYooPayment.CreditIssueDate,
			Amount:                domain.Money(rawYooPayment.Amount),
			YookassaID:            rawYooPayment.YookassaID,
			TechnicalStatus:       rawYooPayment.TechnicalStatus,
			YooCreatedAt:          rawYooPayment.YooCreatedAt,
			ExecutionDateBySystem: rawYooPayment.ExecutionDateBySystem,
			Description:           rawYooPayment.Description,
		})
	}
	if len(response) == 0 {
		y.logger.Warn("all yookassa payments invalid")
		return []domain.YooPayment{}, nil
	}

	y.logger.Info("yookassa payments fetched successfully", "count", len(response))
	return response, nil
}

func (y *yooPaymentProducer) Acknowledge(ctx context.Context, ids []domain.YooPaymentID) error {
	if len(ids) == 0 {
		y.logger.Warn("empty yookassa payment ids batch for Acknowledge")
		return nil
	}

	err := y.http.Patch(ctx, YookassaPath, ackYooPaymentRequest{IDs: ids})
	if err != nil {
		y.logger.Error(
			"failed to insert yookassa payment ids into prod service",
			"err", err,
		)
		return err
	}
	return nil
}

func NewYooPaymentProducer(http *httpclient.HTTPClient, logger *slog.Logger) *yooPaymentProducer {
	return &yooPaymentProducer{http: http, logger: logger}
}
