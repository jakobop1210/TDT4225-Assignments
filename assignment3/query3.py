from pprint import pprint
from DbConnector import DbConnector

def fetch_top_users():
    """
    Fetch the top 20 users with the highest number of activities from the MongoDB database
    and print the results to the console.
    """
    db_connector = DbConnector()
    db = db_connector.db

    try:
        # Aggregate to count activities per user
        pipeline = [
            {
                "$group": {
                    "_id": "$user_id", 
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"activity_count": -1}  # Sort by activity count descending
            },
            {
                "$limit": 20  # Limit to top 20 users
            }
        ]

        top_users = list(db['activities'].aggregate(pipeline))

        # Prepare results for pretty printing
        results = [{"User ID": user["_id"], "Activity Count": user["activity_count"]} for user in top_users]

        print("Top 20 users with the highest number of activities:")
        pprint(results)

    except Exception as e:
        print("ERROR: Failed to fetch top users:", e)

    finally:
        db_connector.close_connection()

if __name__ == "__main__":
    fetch_top_users()
