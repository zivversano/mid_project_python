import pandas as pd
from repositories.extract import extract_data_to_parquet
from repositories.utils import normalize_columns
from repositories.load_postgress import load_postgres
from repositories.transform import clean_data, apply_mapping
from models.mapping import satisfaction_mapping

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
    mapped_data_df = apply_mapping(cleaned_data_df, SATISFACTION_MAPPINGS)
    print(f"Applied mapping to data")
    
    # Convert all object columns to string to avoid parquet type issues
    for col in mapped_data_df.select_dtypes(include=['object']).columns:
        mapped_data_df[col] = mapped_data_df[col].astype(str)
    
    # Save cleaned and mapped data
    print("\n=== SAVING TRANSFORMED DATA ===")
    output_cleaned_path = 'data/output/cleaned_data.parquet'
    mapped_data_df.to_parquet(output_cleaned_path, index=False)
    print(f"Saved cleaned data to {output_cleaned_path}")
    
    # Load to PostgreSQL
    print("\n=== LOADING TO POSTGRESQL ===")
    try:
        load_postgres(output_cleaned_path, table_name='satisfaction_2016_cleaned')
        print("Successfully loaded data to PostgreSQL!")
    except Exception as e:
        print(f"Warning: Could not load to PostgreSQL: {e}")
        print("Data has been saved to parquet file and can be loaded later.")
    
    print("\n=== ETL PIPELINE COMPLETE ===")


if __name__ == "__main__":
    main()