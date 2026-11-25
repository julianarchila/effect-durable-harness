-- The payment gateway's ledger: one refund per idempotency key.
CREATE TABLE refunds (
  idempotency_key TEXT PRIMARY KEY,
  refund_id TEXT NOT NULL,
  charge_id TEXT NOT NULL,
  amount_cents INTEGER NOT NULL
);
