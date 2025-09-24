import os
from pathlib import Path
import pandas as pd
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from backend.read_env import MYSQL_USER,MYSQL_PASSWORD,MYSQL_HOST,MYSQL_PORT
from backend.read_settings import settings_dict

# ========== CONFIG ==========
MYSQL_DB   = "LOG"
TABLE      = "cache_logs"

OUTPUT_DIR = Path("/Users/xavierschaefer/Documents/E-Transmission/ARCHIVE_COMPRESSION/parquet_export/1758712804/LOG.cache_logs")
#OUTPUT_DIR = Path(settings_dict['path_to_export']) / f"{MYSQL_DB}.{TABLE}"

# How many DataFrame rows to insert per executemany batch
INSERT_BATCH_SIZE = 2000
# If True, TRUNCATE the destination table before loading
TRUNCATE_BEFORE_LOAD = False
# File glob pattern for parquet parts (keeps natural sort)
PARQUET_GLOB = "part-*.parquet"
# ============================

def make_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        use_pure=True,
        autocommit=False
    )

def list_parquet_files(output_dir: Path, pattern: str):
    files = sorted([p for p in output_dir.glob(pattern) if p.is_file()])
    return files

def df_to_mysql_rows(df: pd.DataFrame):
    # Replace NaN with None for SQL NULLs
    df = df.where(pd.notnull(df), None)
    # Convert dataframe to list of tuples in column order
    return [tuple(row) for row in df.itertuples(index=False, name=None)]

def build_insert_query(column_names):
    cols_escaped = ", ".join(f"`{c}`" for c in column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    q = f"INSERT INTO `{TABLE}` ({cols_escaped}) VALUES ({placeholders})"
    return q

def infer_columns_from_first_file(first_file: Path):
    df = pd.read_parquet(first_file)
    # keep columns order as in file
    cols = list(df.columns)
    return cols

def truncate_table_if_requested(conn):
    if not TRUNCATE_BEFORE_LOAD:
        return
    cur = conn.cursor()
    print(f"Truncating table `{TABLE}`...")
    cur.execute(f"TRUNCATE TABLE `{TABLE}`")
    conn.commit()
    cur.close()

def insert_dataframe_in_batches(conn, df: pd.DataFrame, insert_q: str):
    cur = conn.cursor()
    rows = df_to_mysql_rows(df)
    total = 0
    # chunk rows to INSERT_BATCH_SIZE to avoid huge queries
    for i in range(0, len(rows), INSERT_BATCH_SIZE):
        batch = rows[i:i+INSERT_BATCH_SIZE]
        try:
            cur.executemany(insert_q, batch)
            conn.commit()
            total += len(batch)
            print(f"  → inserted batch {i // INSERT_BATCH_SIZE + 1}: {len(batch):,} rows (total {total:,})")
        except Exception as e:
            conn.rollback()
            cur.close()
            raise RuntimeError(f"Error inserting batch starting at row {i}: {e}") from e
    cur.close()
    return total

def load_parquet_dir_to_mysql():
    files = list_parquet_files(OUTPUT_DIR, PARQUET_GLOB)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {OUTPUT_DIR} with pattern {PARQUET_GLOB}")
    print(f"Found {len(files)} parquet files. First: {files[0].name}, Last: {files[-1].name}")

    # infer columns/order from first file
    column_names = infer_columns_from_first_file(files[0])
    insert_q = build_insert_query(column_names)
    print(f"Inferred {len(column_names)} columns: {column_names[:5]}{'...' if len(column_names)>5 else ''}")
    print("Insert query:", insert_q[:200], "..." if len(insert_q)>200 else "")

    conn = make_conn()
    try:
        truncate_table_if_requested(conn)
        total_inserted = 0

        for f in files:
            print(f"Reading {f.name} ...")
            df = pd.read_parquet(f)
            # ensure df columns are exactly in the inferred order (some parquet writers can reorder)
            df = df.reindex(columns=column_names)
            if df.shape[0] == 0:
                print("  (empty file, skipped)")
                continue
            inserted = insert_dataframe_in_batches(conn, df, insert_q)
            total_inserted += inserted
            print(f"Finished {f.name}: {inserted:,} rows inserted.")
        print(f"All done. Total rows inserted: {total_inserted:,}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("Starting Parquet -> MySQL import")
    print("OUTPUT_DIR:", OUTPUT_DIR)
    try:
        load_parquet_dir_to_mysql()
    except Exception as e:
        print("Import failed:", str(e))
        raise

