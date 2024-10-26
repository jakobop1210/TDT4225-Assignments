from pprint import pprint
from DbConnector import DbConnector

def average_activities_per_user():
    """
    Calculate the average number of activities per user in the MongoDB database
    and print the result to the console.
    """
    connection = DbConnector()
    db = connection.db

    # Count total users and activities
    number_of_users = db['users'].count_documents({})
    number_of_activities = db['activities'].count_documents({})

    # Calculate the average number of activities per user
    average_activities = number_of_activities / number_of_users if number_of_users > 0 else 0

    # Prepare output as a dictionary
    results = {
        "Number of users": number_of_users,
        "Number of activities": number_of_activities,
        "Average activities per user": average_activities
    }

    # Pretty print the results
    pprint(results)

    connection.close_connection()

if __name__ == "__main__":
    average_activities_per_user()