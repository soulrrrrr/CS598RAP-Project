import duckdb
import os
import shutil

def generate_tpcds_data(data_gb: int):
    duckdb.sql("INSTALL tpcds")
    duckdb.sql("LOAD tpcds")
    duckdb.sql(f"CALL dsdgen(sf = {data_gb})")

def generate_duckdb_data():
    # init ducklake
    ducklake_path = "ducklake"
    metadata_file_path = "metadata.ducklake"
    lake_name = "my_ducklake"
    data_files_path = "data_files"

    # ensure clean data dir
    if os.path.exists(data_files_path):
        shutil.rmtree(data_files_path)
        print(f"[WARN] Removed existing data directory: {data_files_path}")

    # if metadata exists, remove it (clean rebuild)
    if os.path.exists(metadata_file_path):
        os.remove(metadata_file_path)
        print(f"[WARN] Removed existing metadata file: {metadata_file_path}")
    
    # ducklake operations
    duckdb.sql("INSTALL ducklake")
    duckdb.sql("LOAD ducklake")
    duckdb.sql(f"ATTACH 'ducklake:{metadata_file_path}' AS {lake_name} (DATA_PATH '{data_files_path}')")
    duckdb.sql(f"COPY from database memory to {lake_name}")

def move_ducklake_data(source_dir="data_files", metadata_file="metadata.ducklake", target_dir="ducklake_tpcds/data_files"):
    # create target directory if it doesn't exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"[OK] Created target directory: {target_dir}")

    # move all files from data directory
    for f in os.listdir(source_dir):
        src_path = os.path.join(source_dir, f)
        dst_path = os.path.join(target_dir, f)
        shutil.move(src_path, dst_path)
    
    # move metadata file
    if os.path.exists(metadata_file):
        shutil.move(metadata_file, os.path.join(target_dir, metadata_file))

    print(f"[OK] Moved DuckLake data to {target_dir}")

def main():
    generate_tpcds_data(1)
    generate_duckdb_data()
    move_ducklake_data()

if __name__ == "__main__":
    main()