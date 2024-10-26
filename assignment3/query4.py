from pprint import pprint
from DbConnector import DbConnector

def find_all_user_taken_taxi():
    """
    Find all distinct users that have taken a taxi in the MongoDB database.
    Print the user IDs to the console.
    """
    connection = DbConnector()
    db = connection.db

    try:
        # Query to find distinct users who have taken a taxi
        taxi_users = db['activities'].distinct("user_id", {"transportation_mode": "taxi"})

        print("Users that have taken a taxi:")
        for user_id in taxi_users:
            print(f"- User ID: {user_id}")

    except Exception as e:
        print("ERROR: Failed to fetch users who have taken a taxi:", e)

    finally:
        connection.close_connection()

if __name__ == "__main__":
    find_all_user_taken_taxi()
