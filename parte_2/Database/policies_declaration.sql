CREATE TABLE
  policies
(
  id TEXT PRIMARY KEY,
  order_id TEXT REFERENCES orders (id),
  insurance_type TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT
);
