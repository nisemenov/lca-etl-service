-- +goose Up
ALTER TABLE payments ADD COLUMN batch_id TEXT;
ALTER TABLE yookassa ADD COLUMN batch_id TEXT;

-- +goose Down
ALTER TABLE payments DROP COLUMN batch_id;
ALTER TABLE yookassa DROP COLUMN batch_id;
