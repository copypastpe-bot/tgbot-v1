-- Client bot onboarding fields

ALTER TABLE IF EXISTS clients
    ADD COLUMN IF NOT EXISTS bot_tg_user_id BIGINT UNIQUE,
    ADD COLUMN IF NOT EXISTS bot_started BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS bot_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS bot_bonus_granted BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS preferred_contact TEXT NOT NULL DEFAULT 'unknown';

