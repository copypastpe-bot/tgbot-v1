#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$PROJECT_DIR/.env}"
LOG_FILE="$PROJECT_DIR/bot.log"
PID_FILE="$PROJECT_DIR/.bot.pid"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "❌ Env file not found: $ENV_FILE"
  echo "Usage: $0 [path_to_env_file]"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

if [[ -z "${DB_DSN:-}" ]]; then
  echo "❌ DB_DSN is not set in $ENV_FILE"
  exit 1
fi

if [[ -z "${BOT_TOKEN:-}" ]]; then
  echo "❌ BOT_TOKEN is not set in $ENV_FILE"
  exit 1
fi

PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

stop_bot() {
  local found=0
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    local cwd
    cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -n1 || true)"
    if [[ "$cwd" == "$PROJECT_DIR" ]]; then
      kill "$pid" 2>/dev/null || true
      found=1
    fi
  done < <(pgrep -f "python.*bot.py" || true)

  if [[ "$found" -eq 1 ]]; then
    sleep 1
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      local cwd
      cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -n1 || true)"
      if [[ "$cwd" == "$PROJECT_DIR" ]]; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    done < <(pgrep -f "python.*bot.py" || true)
    echo "✅ Bot process stopped"
  else
    echo "ℹ️ Bot process was not running"
  fi
}

reset_db() {
  echo "🧹 Resetting database..."
  psql "$DB_DSN" -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE
    table_list text;
BEGIN
    SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
    INTO table_list
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename <> 'alembic_version';

    IF table_list IS NOT NULL AND table_list <> '' THEN
        EXECUTE 'TRUNCATE TABLE ' || table_list || ' RESTART IDENTITY CASCADE';
    END IF;
END $$;
SQL
  echo "✅ Database cleaned"
}

start_bot() {
  echo "🚀 Starting bot..."
  nohup "$PYTHON_BIN" "$PROJECT_DIR/bot.py" >"$LOG_FILE" 2>&1 &
  local bot_pid=$!
  echo "$bot_pid" >"$PID_FILE"
  sleep 2
  if ps -p "$bot_pid" >/dev/null 2>&1; then
    echo "✅ Bot started. PID: $bot_pid"
    echo "📄 Log: $LOG_FILE"
  else
    echo "❌ Bot failed to start. Check log: $LOG_FILE"
    exit 1
  fi
}

echo "Project: $PROJECT_DIR"
echo "Env: $ENV_FILE"
stop_bot
reset_db
start_bot
