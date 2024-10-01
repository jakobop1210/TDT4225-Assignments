from tabulate import tabulate
from DbConnector import DbConnector

def fetch_year_with_most_activities():
    db_connector = DbConnector()

    try:
        # Define the SQL query to find the year with the most activities
        query = """
        SELECT YEAR(start_date_time) AS activity_year, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY activity_year
        ORDER BY activity_count DESC
        LIMIT 1;
        """

        # Execute the query
        db_connector.cursor.execute(query)
        row = db_connector.cursor.fetchone()  # Fetch only one row since we need the top result

        # Print the results in a tabulated format
        if row:
            print("Year with the most activities:")
            print(tabulate([row], headers=["Year", "Activity Count"]))
        else:
            print("No activities found in the database.")

    except Exception as e:
        print("ERROR: Failed to fetch the year with the most activities:", e)

    finally:
        db_connector.close_connection()  # Ensure the connection is closed

if __name__ == "__main__":
    fetch_year_with_most_activities()  # Execute the function if this file is run directly
