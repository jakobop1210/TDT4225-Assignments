from pprint import pprint
from DbConnector import DbConnector

def count_users_activities_trackpoints():
    """
    Count the total number of users, activities, and trackpoints in the MongoDB database
    and print the results to the console.
    """
    connection = DbConnector()
    db = connection.db

    # Count users, activities, and trackpoints
    number_of_users = db['users'].count_documents({})
    number_of_activities = db['activities'].count_documents({})
    number_of_trackpoints = db['trackpoints'].count_documents({})

    # Prepare output as a dictionary
    results = {
        "Number of users": number_of_users,
        "Number of activities": number_of_activities,
        "Number of trackpoints": number_of_trackpoints
    }

    # Pretty print the results
    pprint(results)

    # Close the connection
    connection.close_connection()

if __name__ == "__main__":
    count_users_activities_trackpoints()
