from service.etl_runner import run_etl

if __name__ == "__main__":
    # Define file paths
    values_file = "/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_values_20251112_200630.xlsx"
    data_file = "/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_data_20251112_200630.xlsx"
    info_file = "/home/local_admin/NAYA/mid_project_python/data/raw/satisfaction_2016_info_20251112_200630.xlsx"

    # Run the ETL pipeline
    run_etl(values_file, data_file, info_file)
