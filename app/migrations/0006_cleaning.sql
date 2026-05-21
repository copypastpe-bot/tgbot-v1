-- Клининг: бригадиры, заказы уборки, отдельная касса.
-- Параллельный финансовый контур, не пересекается с таблицами химчистки.
-- Клиенты, бонусы и notification_outbox остаются общими.

CREATE TABLE IF NOT EXISTS cleaning_foremen (
    id         serial PRIMARY KEY,
    tg_user_id bigint UNIQUE,
    fn         text NOT NULL,
    ln         text,
    phone      text,
    is_active  boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cleaning_orders (
    id             serial PRIMARY KEY,
    client_id      integer NOT NULL REFERENCES clients(id),
    foreman_id     integer NOT NULL REFERENCES cleaning_foremen(id),
    address        text NOT NULL,
    total_amount   numeric(12,2) NOT NULL,
    bonuses_used   numeric(12,2) NOT NULL DEFAULT 0,
    bonuses_earned numeric(12,2) NOT NULL DEFAULT 0,
    client_op_id   text UNIQUE,
    happened_at    timestamptz NOT NULL DEFAULT NOW(),
    created_at     timestamptz NOT NULL DEFAULT NOW(),
    deleted_at     timestamptz
);

CREATE INDEX IF NOT EXISTS idx_cleaning_orders_client
    ON cleaning_orders(client_id);
CREATE INDEX IF NOT EXISTS idx_cleaning_orders_happened
    ON cleaning_orders(happened_at);

CREATE TABLE IF NOT EXISTS cleaning_order_payments (
    id         serial PRIMARY KEY,
    order_id   integer NOT NULL REFERENCES cleaning_orders(id) ON DELETE CASCADE,
    method     text NOT NULL,
    amount     numeric(12,2) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cleaning_op_order
    ON cleaning_order_payments(order_id);

CREATE TABLE IF NOT EXISTS cleaning_cashbook (
    id          serial PRIMARY KEY,
    kind        text NOT NULL,    -- 'income'|'expense'|'dividend'|'withdrawal'|'deposit'
    method      text NOT NULL,    -- income: канонические методы; иначе: категория расхода
    amount      numeric(12,2) NOT NULL,
    comment     text,
    order_id    integer REFERENCES cleaning_orders(id),
    happened_at timestamptz NOT NULL DEFAULT NOW(),
    created_at  timestamptz NOT NULL DEFAULT NOW(),
    deleted_at  timestamptz
);

CREATE INDEX IF NOT EXISTS idx_cleaning_cb_kind_method
    ON cleaning_cashbook(kind, method);
CREATE INDEX IF NOT EXISTS idx_cleaning_cb_order
    ON cleaning_cashbook(order_id);
CREATE INDEX IF NOT EXISTS idx_cleaning_cb_happened
    ON cleaning_cashbook(happened_at);
