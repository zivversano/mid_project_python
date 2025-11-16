#!/usr/bin/env bash
# ETL Pipeline Runner
#
# This script activates the project's virtual environment, loads environment
# variables from .env, and runs the ETL pipeline (main.py).
#
# Usage:
#   ./scripts/run_etl.sh
#
# It will:
#   1. Look for .env in the project root; if not found, show instructions
#   2. Activate the project virtualenv
#   3. Export database credentials from .env
#   4. Run main.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
VENV_PATH="${VENV_PATH:-.venv}"
VENV="$PROJECT_ROOT/$VENV_PATH/bin/activate"

# Check if .env exists; if not, show instructions
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    echo ""
    echo "To set up your environment:"
    echo "  1. Copy the template: cp .env.example .env"
    echo "  2. Edit .env with your PostgreSQL credentials:"
    echo "     - POSTGRES_USER"
    echo "     - POSTGRES_PASSWORD"
    echo "     - POSTGRES_HOST (default: localhost)"
    echo "     - POSTGRES_PORT (default: 5432)"
    echo "     - POSTGRES_DB (default: postgres)"
    echo "  3. Run this script again: ./scripts/run_etl.sh"
    exit 1
fi

# Check if virtualenv exists
if [ ! -f "$VENV" ]; then
    echo "Error: Virtual environment not found at $VENV"
    echo "Please create the venv with: python3 -m venv $VENV_PATH"
    exit 1
fi

# Activate venv
source "$VENV"

# Load .env file
set -a  # export all variable assignments
source "$ENV_FILE"
set +a  # stop exporting

echo "Running ETL pipeline..."
echo "  PostgreSQL Host: $POSTGRES_HOST"
echo "  PostgreSQL Port: $POSTGRES_PORT"
echo "  Database: $POSTGRES_DB"
echo ""

# Run the main ETL script
cd "$PROJECT_ROOT"
python main.py

echo ""
echo "ETL pipeline completed successfully!"
