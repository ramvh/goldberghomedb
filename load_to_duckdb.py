import duckdb

def ingest_raw_data(db_path, raw_data_dir):
    try:
        con = duckdb.connect(db_path)
    
        # Powerful DuckDB feature: Use "glob" to grab every CSV in a platform folder
        platforms = ['amazon', 'shopify', 'lazada']
    except duckdb.IOException as exc:
        msg = str(exc)
        if "Conflicting lock is held" in msg:
            print("DuckDB is locked by another process (e.g., DBeaver). Close it and retry.")
        else:
            print(f"Failed to open DuckDB: {msg}")

    try:            
        for platform in platforms:
            path = f"{raw_data_dir}/{platform}/*.csv"
            # union_by_name=True handles cases where Amazon columns change slightly
            con.execute(f"""
                CREATE OR REPLACE TABLE raw_{platform}_orders AS 
                SELECT * FROM read_csv_auto('{path}', union_by_name=TRUE)
            """)
            print(f"🚀 Loaded {platform} data into raw_{platform}_orders")         
    finally:
        con.close()