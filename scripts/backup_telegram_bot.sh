#!/usr/bin/env bash
set -euo pipefail
umask 077

PROJECT_DIR="/opt/telegram-bot"
BACKUP_ROOT="$PROJECT_DIR/backups"
DB_BACKUP_DIR="$BACKUP_ROOT/db"
CFG_BACKUP_DIR="$BACKUP_ROOT/config"

RETENTION_DAYS="${RETENTION_DAYS:-14}"
OFFSITE_ENABLED="${OFFSITE_ENABLED:-false}"
OFFSITE_SSH_TARGET="${OFFSITE_SSH_TARGET:-}"
OFFSITE_DIR="${OFFSITE_DIR:-}"
OFFSITE_RETENTION_DAYS="${OFFSITE_RETENTION_DAYS:-$RETENTION_DAYS}"
OFFSITE_SSH_OPTS="${OFFSITE_SSH_OPTS:-}"

mkdir -p "$DB_BACKUP_DIR" "$CFG_BACKUP_DIR"

timestamp="$(date +%F_%H-%M-%S)"
db_backup_file="$DB_BACKUP_DIR/tgbot_db_${timestamp}.sql.gz"
cfg_backup_file="$CFG_BACKUP_DIR/tgbot_config_${timestamp}.tar.gz"

cd "$PROJECT_DIR"

set -a
source "$PROJECT_DIR/.env"
set +a

if [[ -z "${DB_DSN:-}" ]]; then
  echo "[$(date +'%F %T')] ERROR: DB_DSN is not set in .env" >&2
  exit 1
fi

pg_dump "$DB_DSN" | gzip -9 > "$db_backup_file"
chmod 600 "$db_backup_file"

gzip -t "$db_backup_file"
if [[ ! -s "$db_backup_file" ]]; then
  echo "[$(date +'%F %T')] ERROR: empty DB backup file: $db_backup_file" >&2
  exit 1
fi

files_to_backup=(".env")
if [[ -f ".env.example" ]]; then
  files_to_backup+=(".env.example")
fi
if [[ -d "docs" ]]; then
  files_to_backup+=("docs")
fi

tar -czf "$cfg_backup_file" "${files_to_backup[@]}"
chmod 600 "$cfg_backup_file"

find "$DB_BACKUP_DIR" -type f -name "tgbot_db_*.sql.gz" -mtime +"$RETENTION_DAYS" -delete
find "$CFG_BACKUP_DIR" -type f -name "tgbot_config_*.tar.gz" -mtime +"$RETENTION_DAYS" -delete

if [[ "$OFFSITE_ENABLED" == "true" || "$OFFSITE_ENABLED" == "1" ]]; then
  if [[ -z "$OFFSITE_SSH_TARGET" || -z "$OFFSITE_DIR" ]]; then
    echo "[$(date +'%F %T')] ERROR: offsite enabled but OFFSITE_SSH_TARGET/OFFSITE_DIR not set" >&2
    exit 1
  fi

  ssh_cmd=(ssh)
  if [[ -n "$OFFSITE_SSH_OPTS" ]]; then
    read -r -a offsite_opts <<< "$OFFSITE_SSH_OPTS"
    ssh_cmd+=("${offsite_opts[@]}")
  fi

  "${ssh_cmd[@]}" "$OFFSITE_SSH_TARGET" "mkdir -p '$OFFSITE_DIR/db' '$OFFSITE_DIR/config' && chmod 700 '$OFFSITE_DIR' '$OFFSITE_DIR/db' '$OFFSITE_DIR/config'"

  rsync_ssh="ssh"
  if [[ -n "$OFFSITE_SSH_OPTS" ]]; then
    rsync_ssh="ssh $OFFSITE_SSH_OPTS"
  fi

  rsync -az --chmod=F600,D700 -e "$rsync_ssh" "$db_backup_file" "$OFFSITE_SSH_TARGET:$OFFSITE_DIR/db/"
  rsync -az --chmod=F600,D700 -e "$rsync_ssh" "$cfg_backup_file" "$OFFSITE_SSH_TARGET:$OFFSITE_DIR/config/"

  "${ssh_cmd[@]}" "$OFFSITE_SSH_TARGET" \
    "find '$OFFSITE_DIR/db' -type f -name 'tgbot_db_*.sql.gz' -mtime +$OFFSITE_RETENTION_DAYS -delete; \
     find '$OFFSITE_DIR/config' -type f -name 'tgbot_config_*.tar.gz' -mtime +$OFFSITE_RETENTION_DAYS -delete"
fi

echo "[$(date +'%F %T')] OK: backup created"
echo "DB: $db_backup_file"
echo "CFG: $cfg_backup_file"
