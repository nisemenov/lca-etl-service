package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestYooPayment(t *testing.T) {
	yoo := YooPayment{
		ID:                    1,
		CaseID:                1,
		DebtorID:              1,
		FullName:              "John Doe",
		CreditNumber:          "1",
		CreditIssueDate:       time.Now(),
		Amount:                Money("1.0"),
		YookassaID:            "215d8da0-000f-50be-b000-0003308c89be",
		TechnicalStatus:       "succeeded",
		YooCreatedAt:          time.Now(),
		ExecutionDateBySystem: time.Now(),
		Description:           "sms",
	}

	err := yoo.Validate()
	require.NoError(t, err)
}
