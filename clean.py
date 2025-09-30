import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

tables = ['yellow_tripdata_2024', 'green_tripdata_2024']

# function to clean all 3 tables in duckdb
def clean_tables():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        # initialize cleaning loop
        for table in tables:
            
            # print number of rows before cleaning
            print(f'Number of pre-cleaning rows for {table}: {con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]}')

            # remove all duplicates, only distinct records remain
            con.execute(f"""
               CREATE TABLE {table}_clean AS 
               SELECT DISTINCT * FROM {table};

               DROP TABLE {table};
               ALTER TABLE {table}_clean RENAME TO {table};
            """)
            logger.info(f"Duplicates removed from {table}")
            print(f"Duplicates removed from {table}")
            
            # remove records with 0 passengers, 0 trip distance, or trip distance > 100 miles
            con.execute(f"""
                DELETE FROM {table}
                WHERE passenger_count = 0;

                DELETE FROM {table}
                WHERE trip_distance = 0;

                DELETE FROM {table}
                WHERE trip_distance > 100;
            """)
            logger.info(f"Removed trips with 0 passengers, 0 mile trip distance, and >100 mile trip distance from {table}")
            print(f"Removed trips with 0 passengers, 0 mile trip distance, and >100 mile trip distance from {table}")

            # remove trips that last longer than 24 hours
            if table == 'yellow_tripdata_2024':
                con.execute(f"""
                    DELETE FROM {table}
                    WHERE (tpep_dropoff_datetime - tpep_pickup_datetime) > INTERVAL '1 day';
                """)
                logger.info(f"Day long trips removed from {table}")
                print(f"Day long trips removed from {table}")

                over_24h = con.execute(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL '1 day';
                """).fetchone()[0]

            # one for each taxi dataset due to different column names
            elif table == 'green_tripdata_2024':
                con.execute(f"""
                    DELETE FROM {table}
                    WHERE (lpep_dropoff_datetime - lpep_pickup_datetime) > INTERVAL '1 day';
                """)
                logger.info(f"Day long trips removed from {table}")
                print(f"Day long trips removed from {table}")

                over_24h = con.execute(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE lpep_dropoff_datetime - lpep_pickup_datetime > INTERVAL '1 day';
                """).fetchone()[0]

            # verify that all cleaning steps were successful
            dup_check = con.execute(f"""
                SELECT COUNT(*) FROM {table};
            """).fetchone()[0]

            zero_passengers = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE passenger_count = 0;
            """).fetchone()[0]

            zero_distance = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance = 0;
            """).fetchone()[0]

            big_distance = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance > 100;
            """).fetchone()[0]

            # print summary of cleaning results
            msg = (f"[{table}] Post-cleaning check: "
                    f"{dup_check} total rows, "
                    f"{zero_passengers} zero-passenger trips, "
                    f"{zero_distance} zero-distance trips, "
                    f"{big_distance} >100 mile trips, "
                    f"{over_24h} >24h trips")
            logger.info(msg)
            print(msg)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_tables()