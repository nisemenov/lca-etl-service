package producer

import (
	"time"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/validation"
)

type paymentDTO struct {
	ID                    domain.PaymentID `json:"id" validate:"required"`
	CaseID                int              `json:"case_id" validate:"required"`
	DebtorID              int              `json:"debtor_id" validate:"required"`
	FullName              string           `json:"full_name" validate:"required"`
	CreditNumber          string           `json:"credit_number" validate:"required"`
	CreditIssueDate       time.Time        `json:"credit_issue_date" validate:"required"`
	Amount                string           `json:"amount" validate:"required"`
	DebtAmount            string           `json:"debt_amount" validate:"required"`
	ExecutionDateBySystem time.Time        `json:"execution_date_by_system" validate:"required"`
	Channel               string           `json:"channel" validate:"required"`
}

func (p *paymentDTO) Validate() error {
	return validation.Validate.Struct(p)
}

// fetchPaymentsResponse contains raw data from prod API
type fetchPaymentsResponse struct {
	Data []paymentDTO `json:"data"`
}

type ackPaymentRequest struct {
	IDs []domain.PaymentID `json:"ids"`
}
