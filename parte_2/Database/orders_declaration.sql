CREATE TABLE
  orders
(
  id TEXT PRIMARY KEY,
  sales_channel TEXT NOT NULL,
  insurance_type TEXT NOT NULL,
  client_id TEXT REFERENCES clients (id),
  pricing NUMERIC,
  created_at TEXT NOT NULL,
  updated_at TEXT
);
