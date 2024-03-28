import os
from os.path import join

import requests
from hdfs import InsecureClient


FILES_DIR_PATH = "/tmp/downloads/data"
METADATA_LINK = "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz"
RATINGS_LINK = "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv"
HDFS_URI = os.getenv('HDFS_URI')

hdfs_client = InsecureClient(HDFS_URI, user='hdfs')


def download_file(url: str, file_path: str):
    """Downloads a file from a given URL and saves it to a local path."""
    print(f"Downloading file from {url}")
    try:
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()

        with open(file_path, 'wb') as f:
            f.write(response.content)

    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise


def ensure_hdfs_directory_exists(directory_path: str):
    """Check if a directory exists in HDFS at the specified path, and if not, create it."""
    try:
        if not hdfs_client.content(directory_path, strict=False):
            hdfs_client.makedirs(directory_path)
            print(f"Directory {directory_path} created in HDFS.")
        else:
            print(f"Directory {directory_path} already exists in HDFS.")
    except Exception as e:
        print(f"An error occurred while accessing HDFS: {e}")


def store_in_hdfs(local_path: str, hdfs_path: str):
    """Stores a file from a local path to an HDFS path."""
    try:
        print(f"Uploading file to {hdfs_path}")
        ensure_hdfs_directory_exists(hdfs_path)
        hdfs_client.upload(hdfs_path, local_path, overwrite=True)

    except Exception as e:
        print(f"Error storing file in HDFS {hdfs_path}: {e}")
        raise


def data_consumption():
    """Downloads metadata and ratings files from specified URLs and stores them locally."""
    metadata_file_path = join(FILES_DIR_PATH, 'metadata.json.gz')
    ratings_file_path = join(FILES_DIR_PATH, 'ratings.csv')

    download_file(METADATA_LINK, metadata_file_path)
    download_file(RATINGS_LINK, ratings_file_path)

def data_storage():
    """Uploads locally stored metadata and ratings files to HDFS."""
    metadata_file_path = join(FILES_DIR_PATH, 'metadata.json.gz')
    ratings_file_path = join(FILES_DIR_PATH, 'ratings.csv')

    store_in_hdfs(metadata_file_path, '/data/movies/metadata')
    store_in_hdfs(ratings_file_path, '/data/movies/ratings')
