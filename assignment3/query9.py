from DbConnector import DbConnector
from datetime import timedelta
from pprint import pprint

def fetch_invalid_activities():
    db_connector = DbConnector()
    db = db_connector.db

    invalid_activities_count = {}
    activities_collection = db['activities']
    trackpoints_collection = db['trackpoints']

    # Get all activities
    activities = list(activities_collection.find())
    total_activities = len(activities)

    for idx, activity in enumerate(activities):
        user_id = activity['user_id']
        activity_id = activity['_id']

        print(f"Checking activity {activity_id} for user {user_id} ({idx + 1}/{total_activities})")

        # Fetch trackpoints for the current activity
        trackpoints = list(trackpoints_collection.find({"activity_id": activity_id}))

        # Check for invalid consecutive trackpoints
        invalid_activity = False

        for i in range(1, len(trackpoints)):
            print(f"Checking trackpoint {i} for activity {activity_id}")
            # Get the timestamps for consecutive trackpoints
            prev_time = trackpoints[i - 1]['date_time']
            current_time = trackpoints[i]['date_time']

            # Calculate the time difference
            time_difference = current_time - prev_time

            # Check if the time difference is greater than or equal to 5 minutes
            if time_difference >= timedelta(minutes=5):
                invalid_activity = True
                break

        # Count the invalid activity for the user
        if invalid_activity:
            if user_id in invalid_activities_count:
                invalid_activities_count[user_id] += 1
            else:
                invalid_activities_count[user_id] = 1

    # Print the results using pprint
    print("Invalid activities per user:")
    pprint(invalid_activities_count)

    db_connector.close_connection()

if __name__ == "__main__":
    fetch_invalid_activities()
