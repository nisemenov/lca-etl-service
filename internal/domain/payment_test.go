package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPayment(t *testing.T) {
	payment := Payment{
		ID:                    1,
		CaseID:                1,
		DebtorID:              1,
		FullName:              "John Doe",
		CreditNumber:          "1",
		CreditIssueDate:       time.Now(),
		Amount:                Money("1.0"),
		DebtAmount:            Money("1.0"),
		ExecutionDateBySystem: time.Now(),
		Channel:               "sms",
	}

	err := payment.Validate()
	require.NoError(t, err)
}
