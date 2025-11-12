import pandas as pd
from pathlib import Path    
from datetime import datetime

RAW_DIR = Path(__file__).parent.parent / "raw_data"
EXTRACTED_DIR = Path(__file__).parent.parent / "extracted_data"     

def run_extract():
    """Extract data from raw Excel files and save as CSV."""
    excel_files = RAW_DIR.glob("*.xlsx")
    
    for excel_file in excel_files:
        df = pd.read_excel(excel_file)
        
        # Create a timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{excel_file.stem}_extracted_{timestamp}.csv"
        output_path = EXTRACTED_DIR / output_filename
        
        df.to_csv(output_path, index=False)
        print(f"Extracted data saved to {output_path}")