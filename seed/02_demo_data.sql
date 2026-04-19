-- ─────────────────────────────────────────────────────────────────────────────
-- Demo schema
-- Includes intentionally unused / redundant indexes so the dashboard
-- has something interesting to show out of the box.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Tables ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    email      TEXT UNIQUE NOT NULL,
    country    TEXT        NOT NULL DEFAULT 'FR',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    sku         TEXT UNIQUE NOT NULL,
    name        TEXT        NOT NULL,
    category    TEXT        NOT NULL,
    price_cents INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT         NOT NULL REFERENCES customers(id),
    status      TEXT        NOT NULL,
    amount_cents INT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    id         SERIAL PRIMARY KEY,
    order_id   INT NOT NULL REFERENCES orders(id),
    product_id INT NOT NULL REFERENCES products(id),
    quantity   INT NOT NULL DEFAULT 1
);

-- ── Useful indexes ───────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_orders_customer  ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created   ON orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_order      ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product    ON order_items(product_id);

-- ── Intentionally unused / redundant indexes (for the demo) ─────────────────

-- Redundant: orders already has idx_orders_customer
CREATE INDEX IF NOT EXISTS idx_orders_customer_dup  ON orders(customer_id, status);
-- Never queried column
CREATE INDEX IF NOT EXISTS idx_customers_country    ON customers(country);
-- Redundant with PK
CREATE INDEX IF NOT EXISTS idx_products_id_unused   ON products(id, sku);

-- ── Seed data ────────────────────────────────────────────────────────────────

INSERT INTO customers (name, email, country)
SELECT
    'Customer ' || g,
    'customer' || g || '@example.com',
    (ARRAY['FR','DE','GB','US','ES'])[floor(random()*5)+1]
FROM generate_series(1, 500) g
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (sku, name, category, price_cents)
SELECT
    'SKU-' || g,
    'Product ' || g,
    (ARRAY['Electronics','Clothing','Food','Books','Home'])[floor(random()*5)+1],
    (random() * 10000)::int + 100
FROM generate_series(1, 200) g
ON CONFLICT (sku) DO NOTHING;

INSERT INTO orders (customer_id, status, amount_cents, created_at)
SELECT
    (random() * 499)::int + 1,
    (ARRAY['pending','paid','shipped','refunded','cancelled'])[floor(random()*5)+1],
    (random() * 50000)::int + 100,
    now() - (random() * interval '180 days')
FROM generate_series(1, 5000);

INSERT INTO order_items (order_id, product_id, quantity)
SELECT
    (random() * 4999)::int + 1,
    (random() * 199)::int  + 1,
    (random() * 5)::int    + 1
FROM generate_series(1, 15000);

-- ── Warm up pg_stat_statements with varied queries ───────────────────────────

-- Fast (indexed)
SELECT * FROM orders WHERE customer_id = 1;
SELECT * FROM orders WHERE status = 'paid' LIMIT 100;

-- Slow (seq-scan on unindexed column)
SELECT count(*) FROM orders WHERE amount_cents > 25000;
SELECT * FROM customers WHERE country = 'FR';
SELECT * FROM order_items WHERE quantity > 3;

ANALYZE customers;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
