-- Добавление поддержки канала MAX в Wahelp
-- Приоритет каналов: бот -> MAX -> TG -> WA

ALTER TABLE clients
    ADD COLUMN IF NOT EXISTS wahelp_user_id_max bigint;

-- Индекс для быстрого поиска по user_id_max (если нужно)
-- CREATE INDEX IF NOT EXISTS idx_clients_wahelp_user_id_max ON clients(wahelp_user_id_max) WHERE wahelp_user_id_max IS NOT NULL;

