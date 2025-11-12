import pandas as pd
from pathlib import Path
from datetime import datetime
import zipfile
import re

# Use the project's data/raw folder (existing) and create data/extracted for outputs
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
EXTRACTED_DIR = PROJECT_ROOT / "data" / "extracted"


def _sanitize_sheet_name(name: str) -> str:
    """Sanitize sheet names for use in filenames."""
    # replace spaces with underscores and remove problematic characters
    name = str(name)
    name = name.strip().replace(" ", "_")
    # keep alphanumeric, dash and underscore
    return re.sub(r"[^0-9A-Za-z_\-]", "", name)


def run_extract():
    """Extract data from raw Excel files and save each sheet as a separate .xlsx file.

    For each .xlsx in `data/raw`, this will create one output file per sheet in
    `data/extracted` with the pattern: <original>_<sheetname>_<timestamp>.xlsx
    """
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

    excel_files = RAW_DIR.glob("*.xlsx")

    for excel_file in excel_files:
        # Read all sheets as a dict: {sheet_name: DataFrame}
        # Try openpyxl first (for real .xlsx files). If the file is an old
        # Excel binary (OLE / .xls renamed to .xlsx), openpyxl will raise a
        # BadZipFile; in that case fall back to xlrd which supports .xls.
        try:
            sheets = pd.read_excel(excel_file, sheet_name=None, engine="openpyxl")
        except Exception:
            # fallback to xlrd for legacy Excel files
            sheets = pd.read_excel(excel_file, sheet_name=None, engine="xlrd")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for sheet_name, df in sheets.items():
            safe_sheet = _sanitize_sheet_name(sheet_name) or "sheet"
            output_filename = f"{excel_file.stem}_{safe_sheet}_{timestamp}.xlsx"
            output_path = EXTRACTED_DIR / output_filename

            # Write single-sheet Excel file
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name[:31])

            print(f"Saved sheet '{sheet_name}' to {output_path}")


if __name__ == "__main__":
    run_extract()