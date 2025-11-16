import pandas as pd
from repositories.utils import normalize_columns

def apply_mapping(df:pd.DataFrame,mapping:dict)->pd.DataFrame:
    """Apply value mappings to the DataFrame based on the provided mapping dictionary."""
    for column, map_dict in mapping.items():
        if column in df.columns:
            df[column] = df[column].map(map_dict).fillna(df[column])
    return df

def clean_data(df:pd.DataFrame) -> pd.DataFrame:
    """Perform data cleaning operations on the DataFrame."""
    df = df.drop_duplicates() # drop duplicates
    for col in df.select_dtypes(include=["float64","int64"]).columns:
        df[col] = df[col].fillna(df[col].mean())  # fill numeric columns with mean
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].isnull().sum() > 0:
            df[col] = df[col].fillna(df[col].mode()[0])  # fill categorical columns with mode
    return df
