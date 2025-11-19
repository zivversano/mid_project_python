import argparse
import os
import subprocess
import sys
import pandas as pd
from repositories.extract import extract_data_to_parquet
from repositories.utils import normalize_columns
from repositories.load_postgress import load_postgres, load_postgres_csv
from repositories.transform import clean_data, apply_mapping
from repositories.metadata import build_question_metadata
from repositories.postgres_views import create_readable_view
from models.mapping import satisfaction_mapping
from models.question_texts import build_question_header_map
from models.hospital_scores import compute_hospital_scores, save_hospital_scores_csv

def main():
    # Extract - reads satisfaction_2016 data file and saves to output directory
    print("=== EXTRACTION PHASE ===")
    output_parquet_path = extract_data_to_parquet()
    
    # Load the extracted data
    print("\n=== LOADING EXTRACTED DATA ===")
    data_df = pd.read_parquet(output_parquet_path)
    print(f"Loaded {len(data_df)} rows from {output_parquet_path}")
    
    # Data Exploration
    print("\n=== DATA EXPLORATION (RAW) ===")
    print(f"\nDataset Shape: {data_df.shape[0]} rows × {data_df.shape[1]} columns")
    
    # Data types summary
    print("\n--- Data Types Summary ---")
    dtype_counts = data_df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        print(f"  {dtype}: {count} columns")
    
    # Missing values analysis
    print("\n--- Missing Values Analysis ---")
    null_counts = data_df.isnull().sum()
    null_cols = null_counts[null_counts > 0].sort_values(ascending=False)
    if len(null_cols) > 0:
        print(f"  Columns with missing values: {len(null_cols)}")
        print(f"  Total missing values: {null_counts.sum()}")
        print(f"  Missing percentage: {(null_counts.sum() / (data_df.shape[0] * data_df.shape[1]) * 100):.2f}%")
        print("\n  Top 10 columns with most nulls:")
        for col, null_count in null_cols.head(10).items():
            pct = (null_count / len(data_df)) * 100
            print(f"    {col}: {null_count} ({pct:.1f}%)")
    else:
        print("  No missing values found")
    
    # Numeric columns statistics
    numeric_cols = data_df.select_dtypes(include=['int64', 'float64']).columns
    print(f"\n--- Numeric Columns ({len(numeric_cols)} total) ---")
    if len(numeric_cols) > 0:
        print("  Sample statistics for first 5 numeric columns:")
        for col in numeric_cols[:5]:
            print(f"    {col}: min={data_df[col].min():.2f}, max={data_df[col].max():.2f}, "
                  f"mean={data_df[col].mean():.2f}, median={data_df[col].median():.2f}")
    
    # Categorical columns
    object_cols = data_df.select_dtypes(include=['object']).columns
    print(f"\n--- Categorical Columns ({len(object_cols)} total) ---")
    if len(object_cols) > 0:
        print("  Sample unique value counts for first 5 categorical columns:")
        for col in object_cols[:5]:
            unique_count = data_df[col].nunique()
            print(f"    {col}: {unique_count} unique values")
    
    # Duplicate rows
    duplicate_count = data_df.duplicated().sum()
    print(f"\n--- Data Quality ---")
    print(f"  Duplicate rows: {duplicate_count}")
    if duplicate_count > 0:
        print(f"  Duplicate percentage: {(duplicate_count / len(data_df) * 100):.2f}%")
    
    # Memory usage
    memory_mb = data_df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"  Memory usage: {memory_mb:.2f} MB")
    
    print("\n=== TRANSFORMATION PHASE ===")
    # Clean the data
    cleaned_data_df = clean_data(data_df)
    print(f"Cleaned data: {len(cleaned_data_df)} rows")
    
    # Apply mapping
    mapped_data_df = apply_mapping(cleaned_data_df, satisfaction_mapping)
    print(f"Applied mapping to data")
    
    # Convert all object columns to string to avoid parquet type issues
    for col in mapped_data_df.select_dtypes(include=['object']).columns:
        mapped_data_df[col] = mapped_data_df[col].astype(str)
    
    # Save cleaned and mapped data
    print("\n=== SAVING TRANSFORMED DATA ===")
    output_cleaned_path = 'data/output/cleaned_data.parquet'
    mapped_data_df.to_parquet(output_cleaned_path, index=False)
    print(f"Saved cleaned data to {output_cleaned_path}")

    # Also produce a CSV with Hebrew headers for question columns, for convenience
    print("\n=== GENERATING READABLE HEADERS CSV ===")
    try:
        rename_map = build_question_header_map(list(mapped_data_df.columns), include_code=True)
        readable_df = mapped_data_df.rename(columns=rename_map)
        readable_csv_path = 'data/output/cleaned_data_readable_headers.csv'
        readable_df.to_csv(readable_csv_path, index=False)
        print(f"Saved readable-headers CSV to {readable_csv_path}")
    except Exception as hdr_err:
        print(f"Warning: Failed to generate readable-headers CSV: {hdr_err}")

    # Compute per-hospital averages and overall average
    print("\n=== AGGREGATING HOSPITAL SCORES ===")
    try:
        hospital_scores_df = compute_hospital_scores(mapped_data_df, hospital_col="code_hospital")
        hospital_scores_csv = 'data/output/hospital_scores.csv'
        save_hospital_scores_csv(hospital_scores_df, hospital_scores_csv)
        print(f"Saved hospital scores to {hospital_scores_csv} ({len(hospital_scores_df)} hospitals)")
    except Exception as agg_err:
        print(f"Warning: Failed to compute hospital scores: {agg_err}")
    
    # Build and save question metadata (mapping question codes to human-readable texts)
    print("\n=== BUILDING QUESTION METADATA ===")
    qmeta_df = build_question_metadata(list(mapped_data_df.columns))
    output_qmeta_path = 'data/output/question_texts.parquet'
    qmeta_df.to_parquet(output_qmeta_path, index=False)
    print(f"Saved question metadata to {output_qmeta_path} ({len(qmeta_df)} rows)")
    
    # Load to PostgreSQL
    print("\n=== LOADING TO POSTGRESQL ===")
    try:
        load_postgres(output_cleaned_path, table_name='satisfaction_2016_cleaned')
        # Load question metadata as a separate lookup table
        load_postgres(output_qmeta_path, table_name='question_texts')
        # Load aggregated hospital scores CSV
        load_postgres_csv('data/output/hospital_scores.csv', table_name='hospital_scores')
        # Note: The readable headers CSV is not loaded to Postgres due to column name length limits
        # Use the CSV file directly or the vw_satisfaction_readable view instead
        # Create a readable view with aliased column headers
        create_readable_view(
            source_table='satisfaction_2016_cleaned',
            view_name='vw_satisfaction_readable',
        )
        print("Successfully loaded data and metadata to PostgreSQL and created readable view!")
    except Exception as e:
        print(f"Warning: Could not load to PostgreSQL: {e}")
        print("Data has been saved to parquet file and can be loaded later.")
    
    print("\n=== ETL PIPELINE COMPLETE ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Satisfaction ETL Pipeline")
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install Python dependencies from requirements.txt using the current interpreter",
    )
    parser.add_argument(
        "--requirements",
        default="requirements.txt",
        help="Path to requirements.txt (default: requirements.txt)",
    )
    args = parser.parse_args()

    if args.install_deps:
        req_path = args.requirements
        print(f"Installing dependencies from {req_path} with interpreter: {sys.executable}")
        if not os.path.exists(req_path):
            print(f"Error: requirements file not found at {req_path}")
            sys.exit(1)
        try:
            # Use the current interpreter to run pip for consistent environment install
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", req_path], check=True)
            print("Dependencies installed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install dependencies (exit code {e.returncode}).")
            sys.exit(e.returncode)

    # Run the ETL pipeline
    main()