from pprint import pprint
from DbConnector import DbConnector

def fetch_year_with_most_recorded_hours():
    db_connector = DbConnector()
    db = db_connector.db

    try:
        # Aggregate to find the year with the most recorded hours
        pipeline = [
            {
                "$group": {
                    "_id": {"$year": "$start_date_time"},  # Group by year
                    "total_hours": {
                        "$sum": {
                            "$divide": [
                                {"$subtract": ["$end_date_time", "$start_date_time"]},  # Duration in milliseconds
                                3600000  # Convert milliseconds to hours
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"total_hours": -1}  # Sort by total hours descending
            },
            {
                "$limit": 1  # Get the top result
            }
        ]

        result = list(db['activities'].aggregate(pipeline))

        if result:
            output = {
                "Year": result[0]["_id"],
                "Total Hours": result[0]["total_hours"]
            }
            print("Year with the most recorded hours:")
            pprint(output)
        else:
            print("No activities found in the database.")

    except Exception as e:
        print("ERROR: Failed to fetch the year with the most recorded hours:", e)

    finally:
        db_connector.close_connection()

if __name__ == "__main__":
    fetch_year_with_most_recorded_hours()
