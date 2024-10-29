from DbConnector import DbConnector
from pprint import pprint

def fetch_invalid_activities():
    db_connector = DbConnector()
    db = db_connector.db

    # Aggregation pipeline to find invalid activities per user
    pipeline = [
        {
            "$match": {
                "is_valid": False  # Filter for invalid activities
            }
        },
        {
            "$group": {
                "_id": "$user_id",  # Group by user_id
                "invalid_activity_count": {"$sum": 1}  # Count invalid activities
            }
        }
    ]

    invalid_activities = list(db['activities'].aggregate(pipeline))

    # Transform results into a dictionary
    invalid_activities_count = {item["_id"]: item["invalid_activity_count"] for item in invalid_activities}

    # Print the results using pprint
    print("Invalid activities per user:")
    pprint(invalid_activities_count)

    db_connector.close_connection()

if __name__ == "__main__":
    fetch_invalid_activities()
