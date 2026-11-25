-- The shop's own data, which the agent reads and writes through the consumer's services: external systems
-- belong to the consumer. Seeded with sample orders and staff.
CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  item TEXT NOT NULL,
  total_cents INTEGER NOT NULL,
  refunded_cents INTEGER NOT NULL DEFAULT 0,
  charge_id TEXT NOT NULL,
  placed_at INTEGER NOT NULL
);

CREATE TABLE staff (
  email TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  on_call INTEGER NOT NULL DEFAULT 0
);

-- One row per escalation, keyed by the task's idempotency key, so a retried page writes nothing new.
CREATE TABLE escalations (
  key TEXT PRIMARY KEY,
  ticket_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  paged TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

INSERT INTO orders (id, customer_id, status, item, total_cents, charge_id, placed_at) VALUES
  ('o-1001', 'c-ada', 'shipped', 'Mechanical keyboard', 12900, 'ch_1001', unixepoch('now') * 1000),
  ('o-1002', 'c-ada', 'delivered', 'Desk lamp', 4500, 'ch_1002', (unixepoch('now') - 90 * 86400) * 1000),
  ('o-2001', 'c-grace', 'processing', 'Monitor arm', 8900, 'ch_2001', unixepoch('now') * 1000);

INSERT INTO staff (email, name, on_call) VALUES
  ('lin@shop.example', 'Lin', 1),
  ('sam@shop.example', 'Sam', 0);
