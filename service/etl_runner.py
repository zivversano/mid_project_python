from repositories.extract_values import run_extract_values
from repositories.extract_data import run_extract_data
from repositories.extract_info import run_extract_info
from repositories.transform_clean import run_transform
from repositories.load_postgress import run_load
def run_etl(values_file, data_file, info_file):
    """
    Run the full ETL pipeline: extract, transform, load.
    """
    # Extraction
    parquet_values = run_extract_values(values_file)
    parquet_data = run_extract_data(data_file)
    parquet_info = run_extract_info(info_file)

    # Transformation
    parquet_curated = run_transform([parquet_values, parquet_data, parquet_info])

    # Loading
    if parquet_curated is None:
        raise ValueError("Transformation did not return a parquet file path")
    run_load(str(parquet_curated))

