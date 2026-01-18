import os
from dotenv import load_dotenv
from gdrive_sync import download_folder
from load_to_duckdb import ingest_raw_data

# Load variables from the .env file
load_dotenv("gsheet_id.env")

def run_pipeline():
    # 1. Map the IDs from your .env file
    folders = {
        'Shopee': os.getenv('SHOPEE_FOLDER_ID'),
        'Amazon': os.getenv('AMAZON_FOLDER_ID'),
        'Shopify': os.getenv('SHOPIFY_FOLDER_ID'),
        'Lazada': os.getenv('LAZADA_FOLDER_ID')
    }
    
    # 2. Sync files from GDrive to Local
    for platform, folder_id in folders.items():
        if folder_id:
            local_dir = f"./data/raw/{platform}"
            os.makedirs(local_dir, exist_ok=True)
            print(f"--- Syncing {platform} Orders ---")
            download_folder(folder_id, local_dir)

    # 3. Load from Local to DuckDB
    print("\n--- Ingesting into DuckDB ---")
    print(os.getenv('DATABASE_PATH'), os.getenv('RAW_DATA_PATH'))

    ingest_raw_data(os.getenv('DATABASE_PATH'), os.getenv('RAW_DATA_PATH'))

if __name__ == "__main__":
    run_pipeline()