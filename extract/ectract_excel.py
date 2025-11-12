import pandas as pd
import os
from datetime import datetime

EXCEL_PATH='data/raw/satisfaction_2016.xlsx'

RAW_DATA_DIR='data/raw/sarifaction_2016'
PROCESSED_DATA_DIR='data/processed/satisfaction_2016'       

def extract_excel_sheets(excel_path=EXCEL_PATH,
                         raw_data_dir=RAW_DATA_DIR,
                         processed_data_dir=PROCESSED_DATA_DIR):
    """
    Extracts sheets from an Excel file and saves them as CSV files.
    
    Parameters:
    - excel_path: Path to the Excel file.
    - raw_data_dir: Directory to save raw CSV files.
    - processed_data_dir: Directory to save processed CSV files.
    """
    
    # Create directories if they don't exist
    os.makedirs(raw_data_dir, exist_ok=True)
    os.makedirs(processed_data_dir, exist_ok=True)
    
    # Read the Excel file
    xls = pd.ExcelFile(excel_path)
    
    # Iterate through each sheet in the Excel file
    for sheet_name in xls.sheet_names:
        # Read the sheet into a DataFrame
        df = pd.read_excel(xls, sheet_name=sheet_name)
        
        # Save raw data
        raw_file_path = os.path.join(raw_data_dir, f"{sheet_name}_raw.csv")
        df.to_csv(raw_file_path, index=False)
        
        # Process data (example: drop duplicates)
        processed_df = df.drop_duplicates()
        
        # Save processed data
        processed_file_path = os.path.join(processed_data_dir, f"{sheet_name}_processed.csv")
        processed_df.to_csv(processed_file_path, index=False)
        
        print(f"Extracted and processed sheet: {sheet_name}")
