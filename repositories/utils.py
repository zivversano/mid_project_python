import pandas as pd
import yaml
import os
from dotenv import load_dotenv

def load_config(config_path="config/config.yaml"):
    with open(config_path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)
def load_env():
    load_dotenv()
    return {
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }

def normalize_columns(df:pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip() # remove leading/trailing whitespace
        .str.lower()
        .str.replace(r"\s+", "_", regex=True) # replace spaces with underscores
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True) # remove special characters
    )
    return df
