// Package domain defines core business entities and types
// used across the ETL service.
package domain

import (
	"time"

	"github.com/nisemenov/etl-service/internal/etl"
	"github.com/nisemenov/etl-service/internal/validation"
)

type YooPaymentID int

type YooPayment struct {
	ID                    YooPaymentID `validate:"required"`
	CaseID                int          `validate:"required"`
	DebtorID              int          `validate:"required"`
	FullName              string       `validate:"required"`
	CreditNumber          string       `validate:"required"`
	CreditIssueDate       time.Time    `validate:"required"`
	Amount                Money        `validate:"required"`
	YookassaID            string       `validate:"required"`
	TechnicalStatus       string       `validate:"required"`
	YooCreatedAt          time.Time    `validate:"required"`
	ExecutionDateBySystem time.Time    `validate:"required"`
	Description           string       `validate:"required"`

	Status  etl.EtlStatus
	BatchID string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (y *YooPayment) Validate() error {
	return validation.Validate.Struct(y)
}
