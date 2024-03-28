import os
import pandas as pd
import psycopg2
from hdfs import InsecureClient


HDFS_URI = os.getenv('HDFS_URI')
HDFS_EGRESS_PATH = '/data/egress'
hdfs_client = InsecureClient(HDFS_URI, user='hdfs')

MY_SMALL_DWH_HOST = os.getenv('POSTGRES_HOST')
MY_SMALL_DWH_PORT = os.getenv('POSTGRES_PORT')
MY_SMALL_DWH_DB = os.getenv('POSTGRES_DB')
MY_SMALL_DWH_USER = os.getenv('POSTGRES_USER')
MY_SMALL_DWH_PASSWORD = os.getenv('POSTGRES_PASSWORD')


def connect_to_postgres():
    """Connects to the PostgreSQL database and returns the connection and cursor."""
    conn = psycopg2.connect(
        host=MY_SMALL_DWH_HOST,
        port=MY_SMALL_DWH_PORT,
        database=MY_SMALL_DWH_DB,
        user=MY_SMALL_DWH_USER,
        password=MY_SMALL_DWH_PASSWORD
    )
    cur = conn.cursor()
    return conn, cur


def fetch_data_from_postgres(table_name: str) -> pd.DataFrame:
    """Fetches data from a specified PostgreSQL table and returns it as a pandas DataFrame."""
    conn, cur = connect_to_postgres()
    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    cur.close()
    conn.close()
    return df

def store_data_in_hdfs(dataframe: pd.DataFrame, hdfs_path: str,file_name: str):
    """Stores a pandas DataFrame in HDFS as a CSV file."""
    csv_data = dataframe.to_csv(index=False)
    full_hdfs_path = f"{hdfs_path}/{file_name}"
    with hdfs_client.write(full_hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(csv_data)

def egress_to_hdfs(table_mappings: dict):
    """Egresses data from PostgreSQL tables to HDFS as CSV files."""
    for table_name, file_name in table_mappings.items():
        df = fetch_data_from_postgres(table_name)
        store_data_in_hdfs(df, HDFS_EGRESS_PATH, file_name)
