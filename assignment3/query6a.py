from pprint import pprint
from DbConnector import DbConnector

def fetch_year_with_most_activities():
    """
    Fetch the year with the most activities in the MongoDB database
    and print the result to the console.
    """
    connection = DbConnector()
    db = connection.db

    try:
        # Define the aggregation pipeline to find the year with the most activities
        pipeline = [
            {
                "$group": {
                    "_id": { "$year": "$start_date_time" },  # Group by year
                    "activity_count": { "$sum": 1 }  # Count activities
                }
            },
            {
                "$sort": { "activity_count": -1 }  # Sort by count descending
            },
            {
                "$limit": 1  # Get only the top result
            }
        ]

        # Execute the aggregation
        result = list(db['activities'].aggregate(pipeline))

        # Print the result
        if result:
            year_data = result[0]
            output = {
                "Year": year_data['_id'],
                "Activity Count": year_data['activity_count']
            }
            print("Year with the most activities:")
            pprint(output)
        else:
            print("No activities found in the database.")

    except Exception as e:
        print("ERROR: Failed to fetch the year with the most activities:", e)

    finally:
        connection.close_connection()

if __name__ == "__main__":
    fetch_year_with_most_activities()
