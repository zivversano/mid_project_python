import pandas as pd
from pathlib import Path
from datetime import datetime

def run_extract_data(file_path: str = "/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_data_20251112_200630.xlsx", staging_dir="data/staging"):
    Path(staging_dir).mkdir(parents=True, exist_ok=True)
    # Read the first sheet into a DataFrame
    df = pd.read_excel(file_path, sheet_name=0, engine="openpyxl")
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    df["source"] = "data"
    df["ingested_at"] = pd.Timestamp.utcnow()
    out_file = Path(staging_dir) / f"data_{datetime.utcnow().strftime('%Y%m%d')}.parquet"
    df.to_parquet(out_file, index=False)
    return out_file