import pandas as pd
from pathlib import Path
from models.hospital import get_hospital_name

def run_transform(parquet_files,curated_dir="data/curated"):
    Path(curated_dir).mkdir(parents=True, exist_ok=True)
    dfs=[pd.read_parquet(f) for f in parquet_files]
    df=pd.concat(dfs,ignore_index=True)

    if "score" in df.columns:
        df["normalized_score"] = (df["score"]/df["score"].max()) * 100
        out_file = Path(curated_dir)/"satisfaction_curated.parquet"
        df.to_parquet(out_file,index=False)
        return out_file

    if "code_hospital" in df.columns:
        df["hospital_name"] = df["code_hospital"].apply(get_hospital_name)
        df= df.drop_duplicates() # remove duplicates if any
    num_cols= df.select_dtypes(include=["float64","int64"]).columns
    for col in num_cols:
        df[col]=df[col].fillna(df[col].mean())
    cat_cols= df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        if df[col].isnull().sum()>0:
            df[col]=df[col].fillna(df[col].mode()[0])

    out_file = Path(curated_dir)/"hospital_info_curated.parquet"
    df.to_parquet(out_file,index=False)
    print (f"Saved transformed data to {out_file}")
    return out_file


