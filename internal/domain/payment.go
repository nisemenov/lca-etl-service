// Package domain defines core business entities and types
// used across the ETL service.
package domain

import (
	"time"

	"github.com/nisemenov/etl-service/internal/etl"
	"github.com/nisemenov/etl-service/internal/validation"
)

type PaymentID int

// Money type as string, Decimal(18,2) from API
type Money string

type Payment struct {
	ID                    PaymentID `validate:"required"`
	CaseID                int       `validate:"required"`
	DebtorID              int       `validate:"required"`
	FullName              string    `validate:"required"`
	CreditNumber          string    `validate:"required"`
	CreditIssueDate       time.Time `validate:"required"`
	Amount                Money     `validate:"required"`
	DebtAmount            Money     `validate:"required"`
	ExecutionDateBySystem time.Time `validate:"required"`
	Channel               string    `validate:"required"`

	Status etl.EtlStatus

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (p *Payment) Validate() error {
	return validation.Validate.Struct(p)
}
