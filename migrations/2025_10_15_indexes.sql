-- migrations/2025_10_15_indexes.sql
-- Оптимизация индексов для Telegram-бота (клиенты, касса, заказы)

-- Индекс для поиска клиента по телефону
CREATE UNIQUE INDEX IF NOT EXISTS idx_clients_phone_digits
    ON clients ((regexp_replace(phone, '[^0-9]+', '', 'g')));

-- Индексы для кассовых записей
CREATE INDEX IF NOT EXISTS idx_cashbook_entries_kind_master_happened
    ON cashbook_entries (kind, master_id, happened_at);

CREATE INDEX IF NOT EXISTS idx_cashbook_entries_happened_at
    ON cashbook_entries (happened_at);

-- Индекс для заказов по дате (отчёты и зарплаты)
CREATE INDEX IF NOT EXISTS idx_orders_created_at
    ON orders (created_at);

-- Дополнительно: индекс по мастеру для расчёта ЗП
CREATE INDEX IF NOT EXISTS idx_payroll_items_master
    ON payroll_items (master_id);

-- Проверка существующих записей
-- \d clients
-- \d cashbook_entries
-- \d orders
-- \d payroll_items
