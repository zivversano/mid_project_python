import os
import pandas as pd
from .utils import normalize_columns

# Configuration
RAW_DATA_DIR = "/home/local_admin/NAYA/mid_project_python/data/raw/"
OUTPUT_DIR = "/home/local_admin/NAYA/mid_project_python/data/output/"
DATA_FILE = "satisfaction_2016_data_20251112_200630.xlsx"  # The actual data file (5.1M)


def extract_data_to_parquet() -> str:
    """
    Reads only the satisfaction_2016 data file (satisfaction_2016_data_20251112_200630.xlsx),
    normalizes columns, and saves the result as a Parquet file in the output directory.
    Returns the path to the saved Parquet file.
    """
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Build full path to the data file
    excel_path = os.path.join(RAW_DATA_DIR, DATA_FILE)
    
    # Read the Excel file
    print(f"Reading {excel_path}...")
    df = pd.read_excel(excel_path, engine='openpyxl')
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    
    # Normalize column names
    df = normalize_columns(df)
    
    # Save as Parquet
    output_path = os.path.join(OUTPUT_DIR, "satisfaction_2016_data.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")
    
    return output_path


if __name__ == "__main__":
    # Run extraction when script is executed directly
    output_file = extract_data_to_parquet()
    print(f"\nExtraction complete! Data saved to: {output_file}")