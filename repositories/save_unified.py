import pandas as pd

def save_unified_data(parquet_file:str,csv_path:str):
    df=pd.read_parquet(parquet_file)
    df.to_csv(csv_path,index=False,encoding="utf-8")

