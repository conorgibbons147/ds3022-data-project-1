import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
emissions_path = 'data/vehicle_emissions.csv'

# function to programmatically load all of our data into duckdb
def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        # drop tables if they already exist
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_tripdata_2024;
            DROP TABLE IF EXISTS green_tripdata_2024;
            DROP TABLE IF EXISTS vehicle_emissions;         
        """)
        logger.info("Dropped existing tables if they existed")
        print("Dropped existing tables if they existed")

        # create yellow_tripdata_2024 table if not exists based on first yellow file
        first_yellow_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{months[0]}.parquet'
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS yellow_tripdata_2024 AS
            SELECT * FROM read_parquet('{first_yellow_file}') LIMIT 0;
        """)
        logger.info("Created yellow_tripdata_2024 table")
        print("Created yellow_tripdata_2024 table")

        # create green_tripdata_2024 table based first green file
        first_green_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{months[0]}.parquet'
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS green_tripdata_2024 AS
            SELECT * FROM read_parquet('{first_green_file}') LIMIT 0;
        """)
        logger.info("Created green_tripdata_2024 table")
        print("Created green_tripdata_2024 table")

        # loading Yellow taxi data for all months
        yellow_files = [f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month}.parquet' for month in months]
        for file in yellow_files:
            con.execute(f"""
                INSERT INTO yellow_tripdata_2024
                SELECT * FROM read_parquet('{file}');
            """)
            logger.info(f"Loaded data from {file} into yellow_taxi_2024")
            print(f"Loaded data from {file} into yellow_taxi_2024")

        # loading Green taxi data for all months
        green_files = [f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month}.parquet' for month in months]
        for file in green_files:
            con.execute(f"""
                INSERT INTO green_tripdata_2024
                SELECT * FROM read_parquet('{file}');
            """)
            logger.info(f"Loaded data from {file} into green_taxi_2024")
            print(f"Loaded data from {file} into green_taxi_2024")

        # loading vehicle emissions data from CSV
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS vehicle_emissions 
            AS
            SELECT * FROM read_csv('{emissions_path}');
        """)
        logger.info("Loaded vehicle emissions data")
        print("Loaded vehicle emissions data")

        logger.info("Data loading completed successfully")
        print("Data loading completed successfully")

        # print out counts and columns for each table
        for table in ['yellow_tripdata_2024', 'green_tripdata_2024', 'vehicle_emissions']:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"Table {table} has {count} records")
            print(f"Table {table} has {count} records")
            
            columns = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            col_names = [col[1] for col in columns]  # index 1 is the column name
            logger.info(f"Table {table} columns: {col_names}")
            print(f"Table {table} columns: {col_names}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()