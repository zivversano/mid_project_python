import pandas as pd
from pathlib import Path

def run_transform(parquet_files,curated_dir="data/curated"):
    Path(curated_dir).mkdir(parents=True, exist_ok=True)
    dfs=[pd.read_parquet(f) for f in parquet_files]
    df=pd.concat(dfs,ignore_index=True)

    if "score" in df.columns:
        df["normalized_score"] = (df["score"]/df["score"].max()) * 100
        out_file = Path(curated_dir)/"satisfaction_curated.parquet"
        df.to_parquet(out_file,index=False)
        return out_file
