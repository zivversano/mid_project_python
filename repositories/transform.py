import pandas as pd
from typing import Dict


def apply_mapping(df: pd.DataFrame, satisfaction_mapping: Dict) -> pd.DataFrame:
    """
    Apply provided mapping dictionaries to columns; unmapped values are preserved.
    """
    df_copy = df.copy()
    
    for column, map_dict in satisfaction_mapping.items():
        if column in df_copy.columns:
            # Apply mapping, keeping original values for unmapped items
            df_copy[column] = df_copy[column].map(map_dict).fillna(df_copy[column])
            print(f"Applied mapping to column: {column}")
        else:
            print(f"Warning: Column '{column}' not found in DataFrame")
    
    return df_copy


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data cleaning operations on the DataFrame.
    - Remove duplicate rows
    - Fill numeric nulls with mean
    - Fill categorical nulls with mode (or 'Unknown' if no mode exists)
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    print(f"Starting data cleaning... Initial shape: {df.shape}")
    
    # Make a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Drop duplicates
    initial_rows = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    duplicates_removed = initial_rows - len(df_clean)
    print(f"Removed {duplicates_removed} duplicate rows")
    
    # Fill numeric columns with mean
    numeric_cols = df_clean.select_dtypes(include=["float64", "int64"]).columns
    for col in numeric_cols:
        null_count = df_clean[col].isnull().sum()
        if null_count > 0:
            mean_value = df_clean[col][99]
            df_clean[col] = df_clean[col].fillna(mean_value)
            print(f"Filled {null_count} nulls in '{col}' with mean: {mean_value:.2f}")
    
    # Fill categorical columns with mode (or 'Unknown')
    categorical_cols = df_clean.select_dtypes(include=["object"]).columns
    for col in categorical_cols:
        null_count = df_clean[col].isnull().sum()
        if null_count > 0:
            mode_values = df_clean[col].mode()
            if len(mode_values) > 0:
                fill_value = mode_values[0]
                df_clean[col] = df_clean[col].fillna(fill_value)
                print(f"Filled {null_count} nulls in '{col}' with mode: {fill_value}")
            else:
                df_clean[col] = df_clean[col].fillna("Unknown")
                print(f"Filled {null_count} nulls in '{col}' with 'Unknown'")
    
    print(f"Data cleaning complete. Final shape: {df_clean.shape}")
    return df_clean
