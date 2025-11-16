import pandas as pd
from repositories.utils import normalize_columns

def extract_data(file_path:str="/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_2_20251112_200630.xlsx") -> pd.DataFrame:
    """Extract data from an Excel file and return as a pandas DataFrame."""
    df = pd.read_excel(file_path, engine='openpyxl')
    df = normalize_columns(df)
    return df

def extract_metadata(file_path:str="/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_data_20251112_200630.xlsx") -> pd.DataFrame:
    """Extract metadata from an Excel file and return as a pandas DataFrame."""
    df = pd.read_excel(file_path, engine='openpyxl')
    df = normalize_columns(df)
    return df

def extract_values(file_path: str = "/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_values_20251112_200630.xlsx") -> pd.DataFrame:
    """Extract values mapping from an Excel file and return as a pandas DataFrame."""
    df = pd.read_excel(file_path, engine='openpyxl')
    df = normalize_columns(df)
    return df