package producer

import (
	"time"

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/validation"
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

type yooPaymentDTO struct {
	ID                    domain.YooPaymentID `json:"id" validate:"required"`
	CaseID                int                 `json:"case_id" validate:"required"`
	DebtorID              int                 `json:"debtor_id" validate:"required"`
	FullName              string              `json:"full_name" validate:"required"`
	CreditNumber          string              `json:"credit_number" validate:"required"`
	CreditIssueDate       time.Time           `json:"credit_issue_date" validate:"required"`
	Amount                string              `json:"amount" validate:"required"`
	YookassaID            string              `json:"yookassa_id" validate:"required"`
	TechnicalStatus       string              `json:"technical_status" validate:"required"`
	YooCreatedAt          time.Time           `json:"yoo_created_at" validate:"required"`
	ExecutionDateBySystem time.Time           `json:"execution_date_by_system" validate:"required"`
	Description           string              `json:"description" validate:"required"`
}

func (y *yooPaymentDTO) Validate() error {
	return validation.Validate.Struct(y)
}

// fetchYooPaymentsResponse contains raw data from prod API
type fetchYooPaymentsResponse struct {
	Data []yooPaymentDTO `json:"data"`
}

type ackYooPaymentRequest struct {
	IDs []domain.YooPaymentID `json:"ids"`
}
