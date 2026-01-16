"""
export_to_s3.py
Export des tables PostgreSQL StreamVision vers Amazon S3 (Data Lake RAW)

Format:
- Tables relationnelles → CSV
- Partitionnement par date (YYYY-MM-DD)

Auteur : StreamVision Data Engineering
"""

from datetime import datetime
import psycopg2
import pandas as pd
import boto3
from io import StringIO
import sys

# ============================================================================
# CONFIGURATION A MODIFIER
# ============================================================================

DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "streamvision",   # NOM DE TA BASE
    "user": "postgres",
    "password": "1234"
}

S3_CONFIG = {
    "bucket": "streamvision-data-raw",
    "region": "eu-north-1"
}

# Tables OLTP StreamVision à exporter
TABLES = [
    "users",
    "content",
    "viewing_sessions",
    "ratings",
    "watchlist",
    "subscription_events",
    "search_queries",
    "episodes",
    "episode_viewing"
]

# ============================================================================
# CONNEXIONS
# ============================================================================

def get_db_connection():
    try:
        print("Connexion PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        print(f"ERREUR PostgreSQL : {e}")
        sys.exit(1)


def get_s3_client():
    try:
        print("Connexion AWS S3 avec clés locales...")
        # Hardcoding the credentials directly into the client
        s3 = boto3.client(
            "s3", 
            region_name=S3_CONFIG["region"],
        )
        
        # Vérification du bucket
        s3.head_bucket(Bucket=S3_CONFIG["bucket"])
        print("Connexion S3 réussie")
        return s3
    except Exception as e:
        print(f"ERREUR S3 : {e}")
        sys.exit(1)

# ============================================================================
# EXPORT TABLE CSV
# ============================================================================

def export_table_to_s3(table_name, date_partition):
    print(f"\nExport table : {table_name}")

    conn = get_db_connection()

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        print(f"  {len(df)} lignes extraites")
    except Exception as e:
        print(f"  ERREUR lecture {table_name} : {e}")
        conn.close()
        return

    conn.close()

    if df.empty:
        print("  Table vide — skip")
        return

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_key = (
        f"raw/postgres/{table_name}/"
        f"{date_partition}/"
        f"{table_name}_{date_partition.replace('-', '')}.csv"
    )

    s3 = get_s3_client()

    try:
        s3.put_object(
            Bucket=S3_CONFIG["bucket"],
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"  Upload OK → s3://{S3_CONFIG['bucket']}/{s3_key}")
    except Exception as e:
        print(f"  ERREUR upload S3 : {e}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("EXPORT STREAMVISION : POSTGRESQL → AMAZON S3 (RAW)")
    print("=" * 80)

    date_partition = datetime.now().strftime("%Y-%m-%d")
    print(f"Date de partition : {date_partition}")

    for table in TABLES:
        export_table_to_s3(table, date_partition)

    print("\n" + "=" * 80)
    print("EXPORT TERMINE AVEC SUCCES")
    print("=" * 80)
    print(f"Bucket S3 : s3://{S3_CONFIG['bucket']}/raw/postgres/")

if __name__ == "__main__":
    main()
