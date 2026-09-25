import os
import glob
import pandas as pd
import dlt

# This pipeline expects the Kaggle API to be configured or the CSV files to be present in ./data
DATA_DIR = "./data"

@dlt.source
def olist_ecommerce_source(data_dir=DATA_DIR):
    """
    DLT Source that reads all Olist CSV files and yields them as resources.
    """
    # Check if data exists, if not download it automatically
    if not os.path.exists(data_dir) or not os.listdir(data_dir):
        print(f"Data directory '{data_dir}' is empty. Downloading dataset via Kaggle API...")
        os.makedirs(data_dir, exist_ok=True)
        try:
            import kaggle
            kaggle.api.dataset_download_cli('olistbr/brazilian-ecommerce', path=data_dir, unzip=True)
            print("Download and extraction complete.")
        except Exception as e:
            print(f"Failed to download using Kaggle API. Ensure you have your kaggle.json configured.\nError: {e}")
            return

    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    for file_path in csv_files:
        # Extract table name from filename
        # e.g. olist_customers_dataset.csv -> customers
        filename = os.path.basename(file_path)
        table_name = filename.replace("olist_", "").replace("_dataset.csv", "").replace(".csv", "")
        
        # We define a generator function for each CSV to yield chunks of data
        # This prevents large CSVs from overwhelming memory
        @dlt.resource(name=table_name, write_disposition="replace")
        def load_csv(csv_filepath=file_path):
            chunksize = 10000
            for chunk in pd.read_csv(csv_filepath, chunksize=chunksize):
                yield chunk.to_dict(orient="records")
        
        yield load_csv

if __name__ == "__main__":
    # Create the pipeline connecting to BigQuery
    pipeline = dlt.pipeline(
        pipeline_name='olist_ecommerce',
        destination='bigquery',
        dataset_name='ecommerce_bronze' # Medallion Bronze Layer
    )
    
    print("Running DLT Pipeline to load Kaggle Olist data to BigQuery...")
    # Run the pipeline
    load_info = pipeline.run(olist_ecommerce_source())
    
    # Print the outcome
    print(load_info)
