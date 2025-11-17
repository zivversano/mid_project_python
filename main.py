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