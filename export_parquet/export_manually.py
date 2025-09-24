import os
from pathlib import Path
import pandas as pd
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import sys
import datetime as dt

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from backend.read_env import MYSQL_USER,MYSQL_PASSWORD,MYSQL_HOST,MYSQL_PORT
from backend.read_settings import settings_dict

# ==== User inputs ====  
MYSQL_DB   = "LOG"
TABLE      = "cache_logs"

# Dossier de sortie Parquet (sera créé si absent)
OUTPUT_DIR = Path(settings_dict['path_to_export']) / str(round(dt.datetime.timestamp(dt.datetime.now()))) /f"{MYSQL_DB}.{TABLE}"
CHUNK_SIZE = 100  # lignes par lot (ajustez selon RAM)
# ==============================



def mysql_count_rows():
    c.execute(f"SELECT COUNT(*) FROM `{TABLE}`")
    return c.fetchall()

def export_to_parquet():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Option 1 : requête simple ; Option 2 : ajouter un WHERE si besoin
    sql = f"SELECT * FROM `{TABLE}`"

    total_written = 0
    part_idx = 0

    # Astuce : on force pandas à utiliser pyarrow pour les dtypes modernes
    for chunk in pd.read_sql(sql, DB, chunksize=CHUNK_SIZE):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        # écriture en fichiers part-*.parquet pour faciliter la lecture/partition
        out_file = OUTPUT_DIR / f"part-{part_idx:05d}.parquet"
        pq.write_table(
            table,
            out_file,
            # Compression moderne efficace
            compression="zstd",
            use_dictionary=True
        )
        total_written += len(chunk)
        part_idx += 1

    return total_written

def parquet_count_rows() -> int:
    dataset = ds.dataset(str(OUTPUT_DIR), format="parquet")
    # Compte rapide sans tout charger
    return sum(fragment.count_rows() for fragment in dataset.get_fragments())

def truncate_table():
    c.execute(f"TRUNCATE TABLE `{TABLE}`")

if __name__ == "__main__":
    DB = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        port=MYSQL_PORT,
        passwd = MYSQL_PASSWORD,
        database = MYSQL_DB,
        use_pure = True
    )
    c = DB.cursor()
    mysql_rows = mysql_count_rows()[0][0]
    
    print(f"- Lignes MySQL avant export : {mysql_rows}")

    print(f"Export vers {OUTPUT_DIR}…")
    written_rows = export_to_parquet()
    print(f"- Lignes écrites en Parquet : {written_rows}")

    print("Vérification du compte côté Parquet…")
    pq_rows = parquet_count_rows()
    print(f"- Lignes visibles en Parquet : {pq_rows}")

    if pq_rows != mysql_rows:
        raise RuntimeError(
            f"Mismatch: Parquet={pq_rows} vs MySQL={mysql_rows}. "
            "Abandon du truncate pour sécurité."
        )

    print("Les comptes correspondent. Tronquage de la table")
    truncate_table()
    print("Finished Job")
