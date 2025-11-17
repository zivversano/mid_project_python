import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables (override OS env to ensure .env takes precedence)
load_dotenv(override=True)


def get_postgres_engine():
    """
    Return a SQLAlchemy engine for PostgreSQL using environment variables:
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB.
    Defaults provided for local development.
    """
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)


def load_postgres(parquet_file_path: str, table_name: str = "satisfaction_data"):
    """
    Load data from a parquet file into PostgreSQL.
    
    Args:
        parquet_file_path: Path to the parquet file
        table_name: Name of the table to create/replace in PostgreSQL
    """
    print(f"Loading data from {parquet_file_path} to PostgreSQL table '{table_name}'...")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file_path)
    print(f"Read {len(df)} rows from parquet file")
    
    # Get the database engine
    engine = get_postgres_engine()
    
    # Load data to PostgreSQL (replace if table exists)
    with engine.connect() as connection:
        df.to_sql(table_name, connection, if_exists='replace', index=False)
    
    print(f"Successfully loaded {len(df)} rows to table '{table_name}'")


def load_postgres_csv(csv_file_path: str, table_name: str) -> None:
    """Load data from a CSV file into PostgreSQL.

    Args:
        csv_file_path: Path to the CSV file
        table_name: Name of the table to create/replace in PostgreSQL
    """
    print(f"Loading data from {csv_file_path} to PostgreSQL table '{table_name}'...")

    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    print(f"Read {len(df)} rows from csv file")

    # Get the database engine
    engine = get_postgres_engine()

    # Load data to PostgreSQL (replace if table exists)
    with engine.connect() as connection:
        df.to_sql(table_name, connection, if_exists='replace', index=False)

    print(f"Successfully loaded {len(df)} rows to table '{table_name}'")
