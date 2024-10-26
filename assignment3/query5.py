from pprint import pprint
from DbConnector import DbConnector

def find_activities_for_all_modes():
    """
    Find all types of transportation modes in the MongoDB database and count how many activities
    are tagged with these transportation mode labels. Do not count the rows where the mode is null.
    """
    connection = DbConnector()
    db = connection.db

    try:
        # Aggregate to count activities per transportation mode, excluding nulls
        pipeline = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None}  # Exclude null transportation modes
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "activity_count": {"$sum": 1}  # Count activities
                }
            },
            {
                "$sort": {
                    "activity_count": -1  # Sort by count descending
                }
            }
        ]

        result = list(db['activities'].aggregate(pipeline))

        if result:
            # Prepare output as a list of dictionaries for better readability
            output = [{ "Transportation Mode": item['_id'], "Activity Count": item['activity_count'] } for item in result]
            print("Activity counts by transportation mode:")
            pprint(output)
        else:
            print("No transportation modes found")

    except Exception as e:
        print("ERROR: Failed to fetch transportation modes:", e)

    finally:
        connection.close_connection()

if __name__ == '__main__':
    find_activities_for_all_modes()
