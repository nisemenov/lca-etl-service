package consumer

import (
	"encoding/json"

	"github.com/nisemenov/etl-service/internal/domain"
)

func PaymentToClickHouseRow(payment domain.Payment) ([]byte, error) {
	row := map[string]any{
		"case_id":                  payment.CaseID,
		"debtor_id":                payment.DebtorID,
		"full_name":                payment.FullName,
		"credit_number":            payment.CreditNumber,
		"credit_issue_date":        payment.CreditIssueDate.Format("2006-01-02 15:04:05.000"),
		"amount":                   payment.Amount,
		"debt_amount":              payment.DebtAmount,
		"execution_date_by_system": payment.ExecutionDateBySystem.Format("2006-01-02 15:04:05.000"),
		"channel":                  payment.Channel,
	}
	return json.Marshal(row)
}

func YooPaymentToClickHouseRow(yoo domain.YooPayment) ([]byte, error) {
	row := map[string]any{
		"case_id":                  yoo.CaseID,
		"debtor_id":                yoo.DebtorID,
		"full_name":                yoo.FullName,
		"credit_number":            yoo.CreditNumber,
		"credit_issue_date":        yoo.CreditIssueDate.Format("2006-01-02 15:04:05.000"),
		"amount":                   yoo.Amount,
		"yookassa_id":              yoo.YookassaID,
		"technical_status":         yoo.TechnicalStatus,
		"yoo_created_at":           yoo.YooCreatedAt.Format("2006-01-02 15:04:05.000"),
		"execution_date_by_system": yoo.ExecutionDateBySystem.Format("2006-01-02 15:04:05.000"),
		"description":              yoo.Description,
	}
	return json.Marshal(row)
}
