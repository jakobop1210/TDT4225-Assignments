from tabulate import tabulate
from DbConnector import DbConnector

def fetch_year_with_most_recorded_hours():
    db_connector = DbConnector()

    try:
        query = """
        SELECT YEAR(start_date_time) AS activity_year,
               SUM(TIMESTAMPDIFF(SECOND, start_date_time, end_date_time)) / 3600 AS total_hours
        FROM Activity
        GROUP BY activity_year
        ORDER BY total_hours DESC
        LIMIT 1;
        """

        db_connector.cursor.execute(query)
        row = db_connector.cursor.fetchone()

        if row:
            print("Year with the most recorded hours:")
            print(tabulate([row], headers=["Year", "Total Hours"]))
        else:
            print("No activities found in the database.")

    except Exception as e:
        print("ERROR: Failed to fetch the year with the most recorded hours:", e)

    finally:
        db_connector.close_connection()

if __name__ == "__main__":
    fetch_year_with_most_recorded_hours()
