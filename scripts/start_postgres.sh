#!/usr/bin/env bash
set -euo pipefail

# Start (or recreate) the Postgres container defined in docker-compose.yml
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR%/scripts}"
cd "$PROJECT_ROOT"

if ! command -v docker &>/dev/null; then
  echo "Docker is not installed. Please install Docker before running this script." >&2
  exit 1
fi

echo "Bringing up postgres container (midproj-postgres)..."
docker compose up -d postgres

echo "Waiting for healthcheck..."
ATTEMPTS=20
for i in $(seq 1 $ATTEMPTS); do
  STATUS=$(docker inspect -f '{{.State.Health.Status}}' midproj-postgres 2>/dev/null || echo 'starting')
  if [ "$STATUS" = "healthy" ]; then
    echo "Postgres is healthy!"
    break
  else
    echo "[$i/$ATTEMPTS] status: $STATUS"
    sleep 2
  fi
  if [ $i -eq $ATTEMPTS ]; then
    echo "Postgres did not become healthy in time." >&2
    docker logs midproj-postgres | tail -n 50
    exit 1
  fi
done

echo "Testing connection..."
export PGPASSWORD=devpass
psql -U postgres -h localhost -p 5433 -d satisfaction -c "SELECT 'connected' as status;" || {
  echo "Connection test failed." >&2
  exit 1
}

echo "Postgres container ready and database satisfaction accessible."
