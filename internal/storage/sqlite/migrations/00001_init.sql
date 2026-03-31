-- +goose Up
CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER,
    debtor_id INTEGER,
    full_name TEXT,
    credit_number TEXT,
    credit_issue_date DATETIME,
    amount TEXT,
    debt_amount TEXT,
    execution_date_by_system DATETIME,
    channel TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);

-- +goose Down
DROP TABLE payments;
