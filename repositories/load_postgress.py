import pandas as pd
from sqlalchemy import create_engine
from repositories.utils import load_env

def load_postgres(parquet_path:str,table_name:str):
    """Load data from a parquet file into a PostgreSQL database table."""
    # Load environment variables
    env = load_env()
    user = env["POSTGRES_USER"]
    password = env["POSTGRES_PASSWORD"]
    host = env["POSTGRES_HOST"]
    port = env["POSTGRES_PORT"]
    db = env["POSTGRES_DB"]

    # Create database connection string
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_str)

    # Read parquet file into DataFrame
    df = pd.read_parquet(parquet_path)

    # Load DataFrame into PostgreSQL table
    with engine.connect() as connection:
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
    print(f"Data loaded into table '{table_name}' in PostgreSQL database '{db}'.")