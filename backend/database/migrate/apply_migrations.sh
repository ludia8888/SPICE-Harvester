#!/usr/bin/env sh
set -eu

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-spiceadmin}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-spicepass123}"
POSTGRES_DB="${POSTGRES_DB:-spicedb}"

export PGPASSWORD="$POSTGRES_PASSWORD"

echo "[db-migrations] waiting for postgres (${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB})..."
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 1
done

echo "[db-migrations] ensuring extensions + schema_migrations table..."
psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
  migration_id TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SQL

if [ ! -d /migrations ]; then
  echo "[db-migrations] no /migrations directory mounted; nothing to apply."
  exit 0
fi

echo "[db-migrations] applying migrations..."
for file in $(ls -1 /migrations/*.sql 2>/dev/null | sort); do
  name="$(basename "$file")"
  applied="$(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM schema_migrations WHERE migration_id = '$name'" | tr -d '[:space:]')"
  if [ "$applied" = "1" ]; then
    echo "[db-migrations] skip $name (already applied)"
    continue
  fi

  echo "[db-migrations] apply $name"
  psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
BEGIN;
\\i $file
COMMIT;
SQL
  psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "INSERT INTO schema_migrations(migration_id) VALUES ('$name') ON CONFLICT DO NOTHING"
done

echo "[db-migrations] done."

