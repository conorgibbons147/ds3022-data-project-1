import duckdb
import logging
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

trip_tables = ['yellow_tripdata_2024', 'green_tripdata_2024']
dow_names = {0:'Sun', 1:'Mon', 2:'Tue', 3:'Wed', 4:'Thu', 5:'Fri', 6:'Sat'}
month_names = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}

# function to analyze taxi tables in duckdb
def analyze_tables():
    con = None
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        # initialize analysis loop
        for table in trip_tables:

            # Maximum CO2 trip
            max_co2 = con.execute(f"""
                SELECT MAX(trip_co2_kgs) 
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL;
            """).fetchone()[0]
            logger.info(f"Largest CO2 trip in {table}: {max_co2:.3f} kg")
            print(f"Largest CO2 trip in {table}: {max_co2:.3f} kg")

            # Heaviest and lightest hour of day for CO2
            max_row_hour = con.execute(f"""
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL
                GROUP BY hour_of_day
                ORDER BY avg_co2 DESC
                LIMIT 1;
            """).fetchone()
            min_row_hour = con.execute(f"""
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL
                GROUP BY hour_of_day
                ORDER BY avg_co2 ASC
                LIMIT 1;
            """).fetchone()
            logger.info(f"[{table}] Heaviest hour: {max_row_hour[0]} (avg {max_row_hour[1]:.3f} kg) | "
                        f"Lightest hour: {min_row_hour[0]} (avg {min_row_hour[1]:.3f} kg)")
            print(f"[{table}] Heaviest hour: {max_row_hour[0]} (avg {max_row_hour[1]:.3f} kg) | "
                  f"Lightest hour: {min_row_hour[0]} (avg {min_row_hour[1]:.3f} kg)")

            # Heaviest and lightest day of week for CO2
            max_row_day = con.execute(f"""
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL
                GROUP BY day_of_week
                ORDER BY avg_co2 DESC
                LIMIT 1;
            """).fetchone()
            min_row_day = con.execute(f"""
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL
                GROUP BY day_of_week
                ORDER BY avg_co2 ASC
                LIMIT 1;
            """).fetchone()
            max_dow, max_avg = max_row_day
            min_dow, min_avg = min_row_day
            logger.info(f"[{table}] Heaviest day: {dow_names.get(max_dow, max_dow)} (avg {max_avg:.3f} kg) | "
                        f"Lightest day: {dow_names.get(min_dow, min_dow)} (avg {min_avg:.3f} kg)")
            print(f"[{table}] Heaviest day: {dow_names.get(max_dow, max_dow)} (avg {max_avg:.3f} kg) | "
                  f"Lightest day: {dow_names.get(min_dow, min_dow)} (avg {min_avg:.3f} kg)")

            # Heaviest and lightest week of year for CO2
            max_row_week = con.execute(f"""
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL AND week_of_year BETWEEN 1 AND 52
                GROUP BY week_of_year
                ORDER BY avg_co2 DESC
                LIMIT 1;
            """).fetchone()
            min_row_week = con.execute(f"""
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL AND week_of_year BETWEEN 1 AND 52
                GROUP BY week_of_year
                ORDER BY avg_co2 ASC
                LIMIT 1;
            """).fetchone()
            max_week, max_avg = max_row_week
            min_week, min_avg = min_row_week
            logger.info(f"[{table}] Heaviest week: {max_week} (avg {max_avg:.3f} kg) | "
                        f"Lightest week: {min_week} (avg {min_avg:.3f} kg)")
            print(f"[{table}] Heaviest week: {max_week} (avg {max_avg:.3f} kg) | "
                  f"Lightest week: {min_week} (avg {min_avg:.3f} kg)")

            # Heaviest and lightest month of year for CO2
            max_row_month = con.execute(f"""
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL AND month_of_year BETWEEN 1 AND 12
                GROUP BY month_of_year
                ORDER BY avg_co2 DESC
                LIMIT 1;
            """).fetchone()
            min_row_month = con.execute(f"""
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE trip_co2_kgs IS NOT NULL AND month_of_year BETWEEN 1 AND 12
                GROUP BY month_of_year
                ORDER BY avg_co2 ASC
                LIMIT 1;
            """).fetchone()
            max_month, max_avg = max_row_month
            min_month, min_avg = min_row_month
            logger.info(f"[{table}] Heaviest month: {month_names.get(max_month, max_month)} (avg {max_avg:.3f} kg) | "
                        f"Lightest month: {month_names.get(min_month, min_month)} (avg {min_avg:.3f} kg)")
            print(f"[{table}] Heaviest month: {month_names.get(max_month, max_month)} (avg {max_avg:.3f} kg) | "
                  f"Lightest month: {month_names.get(min_month, min_month)} (avg {min_avg:.3f} kg)")

        # Plot monthly totals of CO2 for Yellow vs Green
        y_rows = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM yellow_tripdata_2024
            WHERE trip_co2_kgs IS NOT NULL AND month_of_year BETWEEN 1 AND 12
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).fetchall()
        g_rows = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM green_tripdata_2024
            WHERE trip_co2_kgs IS NOT NULL AND month_of_year BETWEEN 1 AND 12
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).fetchall()

        y_map = {m: v for m, v in y_rows}
        g_map = {m: v for m, v in g_rows}
        months = list(range(1, 13))
        y_totals = [y_map.get(m, 0.0) for m in months]
        g_totals = [g_map.get(m, 0.0) for m in months]

        month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

        plt.figure(figsize=(10, 6))
        plt.plot(months, y_totals, marker="o", label="Yellow Taxi", color="gold")
        plt.plot(months, g_totals, marker="o", label="Green Taxi", color="green")
        plt.xticks(months, month_labels)
        plt.xlabel("Month")
        plt.ylabel("Total CO2 Emissions (kg)")
        plt.title("Monthly CO2 Totals: Yellow vs Green (2024)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()
        plt.savefig("monthly_co2_totals.png", dpi=300, bbox_inches="tight")
        plt.close()

        logger.info("Saved monthly CO2 totals plot")
        print("Saved monthly CO2 totals plot")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        print(f"Error during analysis: {e}")
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    analyze_tables()
