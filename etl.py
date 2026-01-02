import pandas as pd
import numpy as np
import os
import subprocess
from db_config import get_connection
from local_config import DATA_DIR, KAGGLE_DATASET
from auto_cleaning import before_import_clean, read_file, sql_clean

def download_dataset():
    print("\n=== Downloading Kaggle dataset ===")
    os.makedirs(DATA_DIR, exist_ok=True)

    cmd = f'kaggle datasets download -d {KAGGLE_DATASET} -p "{DATA_DIR}" --unzip --force'
    print(f"Running: {cmd}\n")

    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("Kaggle download failed:")
        print(result.stderr)
    else:
        print("Kaggle data updated in:", DATA_DIR)

        # delete zip
        for fname in os.listdir(DATA_DIR):
            if fname.endswith(".zip"):
                try:
                    os.remove(os.path.join(DATA_DIR, fname))
                    print(f"  removed zip: {fname}")
                except Exception as e:
                    print(f"  failed to delete {fname}: {e}")
        print()

            
def import_csv_to_sqlserver(csv_path, table_name, schema="dbo", chunksize=5000):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True #speedup in pyodbc
          
    full_table = f"{schema}.{table_name}"
    print(f"importing {csv_path} → {full_table} ...")
    
    for chunk in read_file(csv_path, chunksize=chunksize):
        chunk = chunk.astype(object)
        chunk = chunk.where(pd.notnull(chunk), None)# format NAN to None
            
        cols = ",".join(chunk.columns)
        placeholders = ",".join(["?"]*len(chunk.columns))
        sql = f"INSERT INTO {full_table} ({cols}) VALUES ({placeholders})"

        data = [tuple(row) for row in chunk.to_numpy()]
        cursor.executemany(sql, data)
        conn.commit()

        print(f"{len(data)} imported")

    cursor.close()
    conn.close()

    print(f"import complete：{full_table}\n")

def truncate_table(table_name, schema="dbo"):
    conn = get_connection()
    cursor = conn.cursor()

    full_table = f"{schema}.{table_name}"
    print(f"truncating table: {full_table}")

    cursor.execute(f"TRUNCATE TABLE {full_table}")
    conn.commit()

    cursor.close()
    conn.close()


FILES_TO_LOAD = [
    {
        "csv": os.path.join(DATA_DIR, "olist_orders_dataset.csv"),
        "table": "stg_orders",
        "pk": ["order_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_order_items_dataset.csv"),
        "table": "stg_order_items",
        "pk": ["order_id", "order_item_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"),
        "table": "stg_order_payments",
        "pk": ["order_id", "payment_sequential"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"),
        "table": "stg_order_reviews",
        "pk": ["review_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_customers_dataset.csv"),
        "table": "stg_customers",
        "pk": ["customer_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_products_dataset.csv"),
        "table": "stg_products",
        "pk": ["product_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_sellers_dataset.csv"),
        "table": "stg_sellers",
        "pk": ["seller_id"]
    },
    {
        "csv": os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"),
        "table": "stg_geolocation",
        "pk": None
    },
    {
        "csv": os.path.join(DATA_DIR, "product_category_name_translation.csv"),
        "table": "stg_product_category_translation",
        "pk": ["product_category_name"]
    },
]




if __name__ == "__main__":
    print("Downloading dataset...")
    download_dataset()
    from create_staging_tables import create_tables
    create_tables()
    
    for job in FILES_TO_LOAD:
        cleaned_csv=before_import_clean(job['csv'],job['pk']) #preprocessing before importing into Mysql database
        truncate_table(job["table"], schema="dbo")
        import_csv_to_sqlserver(cleaned_csv, job["table"])
        sql_clean(job['table'])#clean sql database