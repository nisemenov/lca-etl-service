-- +goose Up
CREATE TABLE IF NOT EXISTS yookassa (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER,
    debtor_id INTEGER,
    full_name TEXT,
    credit_number TEXT,
    credit_issue_date DATETIME,
    amount TEXT,
    yookassa_id TEXT,
    technical_status TEXT,
    yoo_created_at DATETIME,
    execution_date_by_system DATETIME,
    description TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_yookassa_status ON yookassa(status);

-- +goose Down
DROP TABLE yookassa;
