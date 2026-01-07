import pandas as pd
from pathlib import Path
import numpy as np
from db_config import get_connection

def read_file(path, chunksize=5000):
    try:
        return pd.read_csv(path, chunksize=chunksize, encoding="utf-8")
    except UnicodeDecodeError:
        print(f"[WARN] UTF-8 failed -> use ISO-8859-1 : {path}")
        return pd.read_csv(path, chunksize=chunksize, encoding="ISO-8859-1")

def convert_dtypes_olist(df):  

    # 1) date/datetime
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
        "review_creation_date",
        "review_answer_timestamp",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 2) float
    float_cols = [
        "payment_value",
        "price",
        "freight_value",
        "geolocation_lat",
        "geolocation_lng",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3) int
    int_cols = [
        "payment_sequential",
        "payment_installments",
        "order_item_id",
        "review_score",
        "customer_zip_code_prefix",
        "seller_zip_code_prefix",
        "geolocation_zip_code_prefix",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 4) ID / category->string
    str_cols = [
        "order_id",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "order_status",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "seller_id",
        "seller_city",
        "seller_state",
        "geolocation_city",
        "geolocation_state",
        "payment_type",
        "review_id",
        "review_comment_title",
        "review_comment_message",
    ]
    auto_id_cols = [c for c in df.columns if c.endswith("_id")]
    str_cols = list(set(str_cols + auto_id_cols))

    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 5) standardize nan
    df = df.replace(["", " ", "nan", "NaN", "None"], np.nan)

    return df

def normalize_string_values(df):
   
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA})
        )
    return df
    
def before_import_clean(csv_path, primary_keys=None, chunksize=5000):
    csv_path = Path(csv_path)
    cleaned_path = csv_path.with_name(csv_path.stem + "_clean.csv")

    seen = set()              # store primary keys
    cleaned_chunks = []       # store data in chunks

    print(f"Cleaning file: {csv_path.name}")

    for chunk_idx, chunk in enumerate(read_file(csv_path)): 
        print(f"{len(chunk)} rows loaded")
        #normalize column name
        chunk.columns = (
            chunk.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace(r'\W','',regex=True)
        )

        rename_dict = {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length"
        }
        chunk.rename(columns=rename_dict, inplace=True)

        chunk = normalize_string_values(chunk)#normalize value
        
        chunk = convert_dtypes_olist(chunk)#conver data type

        if primary_keys:
            for pk in primary_keys:
                if pk in chunk.columns:
                    chunk[pk] = (
                        chunk[pk]
                        .where(chunk[pk].notna(), None)
                        .astype(str)
                        .str.strip()
                        .str.lower()
                    )
                    
            keys = chunk[primary_keys].astype(str).agg("||".join, axis=1)

            keep_in_chunk = ~keys.duplicated(keep="first")#drop duplicates in chunks
            keep_global = ~keys.isin(seen)#drop duplicate between chunks

            mask = keep_in_chunk & keep_global
            new_chunk = chunk[mask]

            
            seen.update(keys[mask])
            print(f"removed {len(chunk)-len(new_chunk)} duplicates (global uniq)")
        else:
            new_chunk = chunk

        cleaned_chunks.append(new_chunk)

        
    df = pd.concat(cleaned_chunks, ignore_index=True)
    df.to_csv(cleaned_path, index=False)
    print(f"saved cleaned file to {cleaned_path}, total rows={len(df)}")

    return str(cleaned_path)

def sql_clean(table_name, schema="dbo"):
    conn = get_connection()
    cursor = conn.cursor()

    src = f"{schema}.{table_name}"

    #special: geolocation -> build a deduped/aggregated table by zip_prefix
    if table_name == "stg_geolocation":
        dst = f"{schema}.geolocation_clean"
        print(f"\n=== create clean table: {dst} from {src} (dedupe by zip_prefix) ===")

        # drop existing clean table
        cursor.execute(f"IF OBJECT_ID('{dst}', 'U') IS NOT NULL DROP TABLE {dst};")

        # 1 row per zip_code_prefix
        cursor.execute(
            f"""
            SELECT
                geolocation_zip_code_prefix,
                CAST(AVG(CAST(geolocation_lat AS FLOAT)) AS DECIMAL(9,6)) AS geolocation_lat,
                CAST(AVG(CAST(geolocation_lng AS FLOAT)) AS DECIMAL(9,6)) AS geolocation_lng,
                MAX(geolocation_city) AS geolocation_city,
                MAX(geolocation_state) AS geolocation_state
            INTO {dst}
            FROM {src}
            GROUP BY geolocation_zip_code_prefix;
            """
        )

        # add PK on zip_prefix
        cursor.execute(
            f"""
            ALTER TABLE {dst}
            ADD CONSTRAINT PK_geolocation_clean
            PRIMARY KEY (geolocation_zip_code_prefix);
            """
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"done: {dst}")
        return

    clean_table = table_name.replace("stg_", "") + "_clean"
    dst = f"{schema}.{clean_table}"

    print(f"\n=== create clean table: {dst} from {src} ===")
    # fill na
    # 1) drop clean table if exists
    cursor.execute(f"IF OBJECT_ID('{dst}', 'U') IS NOT NULL DROP TABLE {dst};")

    # 2) copy structure + data (SQL Server way)
    cursor.execute(f"SELECT * INTO {dst} FROM {src};")
    conn.commit()

    # 3) read columns metadata (SQL Server)
    cursor.execute(
        """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            CASE WHEN k.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PK
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
              ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = ?
              AND tc.TABLE_NAME = ?
        ) k
          ON c.COLUMN_NAME = k.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ?
          AND c.TABLE_NAME = ?
        """,
        (schema, clean_table, schema, clean_table)
    )
    columns = cursor.fetchall()

    NUMERIC_TYPES = {"decimal", "numeric", "float", "real"}
    INTEGER_TYPES = {"tinyint", "smallint", "int", "bigint"}
    DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime", "time"}
    TEXT_TYPES = {"text", "ntext"}  

    EXCLUDED_EXACT = {
        "review_comment_title",
        "review_comment_message",
        "product_description",
        "product_name",
        "order_delivered_customer_date",
        "order_delivered_carrier_date",
        "order_approved_at",
        "review_creation_date",
        "review_answer_timestamp",
        "shipping_limit_date"
    }

    def should_skip(col, dtype, is_pk):
        col_l = col.lower()
        dtype_l = dtype.lower()

        if is_pk == 1:
            return True
        if col_l in EXCLUDED_EXACT:
            return True
        if col_l == "id" or col_l.endswith("_id"):
            return True
        if dtype_l in TEXT_TYPES:
            return True
        return False

    for col, dtype, is_pk in columns:
        dtype_l = dtype.lower()

        if should_skip(col, dtype, is_pk):
            print(f"[Skip] {col} ({dtype_l}, pk={is_pk})")
            continue

        # 1) numeric -> avg
        if dtype_l in NUMERIC_TYPES:
            cursor.execute(
                f"""
                UPDATE {dst}
                SET [{col}] = (SELECT AVG(CAST([{col}] AS FLOAT)) FROM {src})
                WHERE [{col}] IS NULL;
                """
            )
            print(f"[Numeric] {col} → fillna with avg")


        # 2) integer -> strict median
        elif dtype_l in INTEGER_TYPES:
            cursor.execute(
                f"""
                SELECT DISTINCT
                    PERCENTILE_CONT(0.5)
                    WITHIN GROUP (ORDER BY [{col}])
                    OVER () AS median_value
                FROM {src}
                WHERE [{col}] IS NOT NULL;
                """
            )
            row = cursor.fetchone()
            if not row:
                continue

            median_val = row[0]
            cursor.execute(
                f"UPDATE {dst} SET [{col}] = ? WHERE [{col}] IS NULL;",
                (median_val,)
            )
            print(f"[Int] {col} → fillna with strict median")

        # 3) date -> real value median (OFFSET)
        elif dtype_l in DATE_TYPES:
            cursor.execute(
                f"SELECT COUNT([{col}]) FROM {src} WHERE [{col}] IS NOT NULL;"
            )
            n = cursor.fetchone()[0]
            if not n:
                continue

            offset = n // 2

            cursor.execute(
                f"""
                SELECT [{col}]
                FROM {src}
                WHERE [{col}] IS NOT NULL
                ORDER BY [{col}]
                OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY;
                """,
                (offset,)
            )
            row = cursor.fetchone()
            if not row:
                continue

            median_val = row[0]
            cursor.execute(
                f"UPDATE {dst} SET [{col}] = ? WHERE [{col}] IS NULL;",
                (median_val,)
            )
            print(f"[Date] {col} → fillna with real median date")


        # 4) others (strings etc.) -> mode
        else:
            cursor.execute(
                f"""
                WITH mode_cte AS (
                    SELECT TOP 1 [{col}] AS v
                    FROM {src}
                    WHERE [{col}] IS NOT NULL AND LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) <> ''
                    GROUP BY [{col}]
                    ORDER BY COUNT(*) DESC
                )
                UPDATE {dst}
                SET [{col}] = (SELECT v FROM mode_cte)
                WHERE [{col}] IS NULL OR LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(MAX)))) = '';
                """
            )
            print(f"[Others] {col} → fillna with mode")

    conn.commit()
    cursor.close()
    conn.close()
