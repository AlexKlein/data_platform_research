import datetime
import gzip
import io
import json
import os
import pandas as pd
import re
from hdfs import InsecureClient
import psycopg2

HDFS_URI = os.getenv('HDFS_URI')
hdfs_client = InsecureClient(HDFS_URI, user='hdfs')

MY_SMALL_DWH_HOST = os.getenv('POSTGRES_HOST')
MY_SMALL_DWH_PORT = os.getenv('POSTGRES_PORT')
MY_SMALL_DWH_DB = os.getenv('POSTGRES_DB')
MY_SMALL_DWH_USER = os.getenv('POSTGRES_USER')
MY_SMALL_DWH_PASSWORD = os.getenv('POSTGRES_PASSWORD')

HDFS_METADATA_PATH = '/data/movies/metadata/metadata.json.gz'
HDFS_RATINGS_PATH = '/data/movies/ratings/ratings.csv'


def read_from_hdfs(file_path: str) -> str:
    """Reads content from a file in HDFS."""
    try:
        if not hdfs_client.status(file_path, strict=False):
            print(f"File does not exist: {file_path}")
            return None

        if file_path.endswith('.gz'):
            with hdfs_client.read(file_path) as reader:
                with gzip.open(reader, 'rt', encoding='utf-8') as gzip_file:
                    content = gzip_file.read()
        else:
            with hdfs_client.read(file_path, encoding='utf-8') as reader:
                content = reader.read()

        return content
    except Exception as e:
        print(f"Error reading from HDFS: {e}")
        return None



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


def parse_and_store_metadata():
    """Parses metadata JSON from HDFS and stores it in the PostgreSQL 'raw_data.metadata' table."""
    row_count = 0
    metadata_content = read_from_hdfs(HDFS_METADATA_PATH)

    if metadata_content is None:
        print("No data found in HDFS.")
        return

    conn, cur = connect_to_postgres()

    cur.execute('TRUNCATE TABLE raw_data.metadata')

    insert_query = 'INSERT INTO raw_data.metadata (json_text, created_at) VALUES (%s, %s);'

    for line in metadata_content.strip().split('\n'):
        line = line.replace("'", '"')
        line = re.sub(r"(\s*:\s*)'([^']+)'(\s*[,}])", r'\1"\2"\3', line)
        try:
            metadata = json.loads(line)
            json_text = json.dumps(metadata)
            cur.execute(insert_query, (json_text, datetime.datetime.now()))
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}\n{json_text}")
            continue

        row_count += 1
        if row_count % 100000 == 0:
            conn.commit()
            row_count = 0

    conn.commit()
    cur.close()
    conn.close()


def parse_and_store_ratings():
    """Parses ratings CSV from HDFS and stores it in the PostgreSQL 'raw_data.ratings' table."""
    row_count = 0
    ratings_csv = read_from_hdfs(HDFS_RATINGS_PATH)
    column_names = ['user_id', 'item', 'rating', 'event_timestamp']
    ratings_df = pd.read_csv(io.StringIO(ratings_csv), names=column_names)

    conn, cur = connect_to_postgres()

    cur.execute('TRUNCATE TABLE raw_data.ratings')

    insert_query = ('INSERT INTO raw_data.ratings (user_id, item, rating, event_timestamp, created_at) '
                    'VALUES (%s, %s, %s, %s, %s);')
    for index, row in ratings_df.iterrows():
        cur.execute(insert_query, (
            row['user_id'],
            row['item'],
            row['rating'],
            row['event_timestamp'],
            datetime.date.today()
        ))
        row_count += 1
        if row_count % 1000 == 0:
            conn.commit()
            row_count = 0

    conn.commit()
    cur.close()
    conn.close()
