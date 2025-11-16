#!/usr/bin/env bash
set -euo pipefail

# Ensure we are in project root (directory containing .env)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR%/scripts}"
cd "$PROJECT_ROOT"

if [ ! -f .env ]; then
  echo "ERROR: .env file not found in project root ($PROJECT_ROOT)." >&2
  exit 1
fi

# Load env vars (export for psql)
set -a
source .env
set +a

: "${POSTGRES_USER:=postgres}"
: "${POSTGRES_PASSWORD:=password}"
: "${POSTGRES_HOST:=localhost}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=satisfaction}"

export PGPASSWORD="$POSTGRES_PASSWORD"

echo "Checking if database '$POSTGRES_DB' exists..."
EXISTS=$(psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -tc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}';" || true)
if echo "$EXISTS" | grep -q 1; then
  echo "Database '$POSTGRES_DB' already exists. Skipping creation."
else
  echo "Creating database '$POSTGRES_DB'..."
  psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -c "CREATE DATABASE \"${POSTGRES_DB}\";"
  echo "Database '$POSTGRES_DB' created successfully."
fi

echo "Done."
