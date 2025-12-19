-- Перемыв: поля для отслеживания перемывов заказов

ALTER TABLE IF EXISTS orders
    ADD COLUMN IF NOT EXISTS rewash_flag BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS rewash_marked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rewash_marked_by_master_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS rewash_followup_scheduled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rewash_result SMALLINT,  -- 1 = устранено, 2 = осталось
    ADD COLUMN IF NOT EXISTS rewash_result_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rewash_cycle INTEGER NOT NULL DEFAULT 1;  -- номер попытки перемыва

-- Индекс для быстрого поиска перемывов по мастеру и месяцу
CREATE INDEX IF NOT EXISTS idx_orders_rewash_master_month 
    ON orders(rewash_marked_by_master_id, rewash_marked_at) 
    WHERE rewash_flag = true;

