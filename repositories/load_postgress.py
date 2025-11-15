"""Load a parquet file into Postgres.

This module ensures the project root is on sys.path so local packages
like `data_base` can be imported even when the script is executed by
absolute path (e.g. `/path/to/.venv/bin/python repositories/load_postgress.py`).
"""

from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
# Ensure project root is first on sys.path so `import data_base` works
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import pandas as pd
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Missing dependency 'pandas'. Run using the project venv: .venv/bin/python repositories/load_postgress.py"
    )

try:
    from data_base.connection import get_postgres_engine
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Could not import 'data_base.connection'. Make sure you run the script from the project root or use the project's python:\n"
        ".venv/bin/python repositories/load_postgress.py"
    )

def run_load(parquet_file: str, table_name: str = "satisfaction_curated"):
    """
    Load the curated parquet file into a PostgreSQL database table.
    """
    # Read the parquet file into a DataFrame
    df = pd.read_parquet(parquet_file)

    # Get the database engine
    engine = get_postgres_engine()

    # Load the DataFrame into the specified table in PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)
