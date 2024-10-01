from tabulate import tabulate
from DbConnector import DbConnector

def print_table(data, headers):
    """
    Print a table with the given data and headers
    """
    print(tabulate(data, headers=headers, tablefmt="fancy_grid"))


def print_first_10_records():
    """
    Print the first 10 users, activities, and trackpoints from the database.
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    # SQL query to fetch the first 10 users
    user_query = "SELECT * FROM User LIMIT 10"
    cursor.execute(user_query)
    users = cursor.fetchall()

    # SQL query to fetch the first 10 activities
    activity_query = "SELECT * FROM Activity LIMIT 10"
    cursor.execute(activity_query)
    activities = cursor.fetchall()

    # SQL query to fetch the first 10 trackpoints
    trackpoint_query = "SELECT * FROM TrackPoint LIMIT 10"
    cursor.execute(trackpoint_query)
    trackpoints = cursor.fetchall()

    # Print results using tabulate
    print(tabulate(users, headers=["User ID", "Has Labels"], tablefmt="fancy_grid"))  # Adjust headers based on your User table structure
    print(tabulate(activities, headers=["Activity ID", "User ID", "Transportation Mode", "Start Date Time", "End Date Tine"], tablefmt="fancy_grid"))  # Adjust headers based on your Activity table structure
    print(tabulate(trackpoints, headers=["Trackpoint ID", "Activity ID", "Latitude", "Longitude", "Altitude", "Date Days", "Date Time"], tablefmt="fancy_grid"))  # Adjust headers based on your TrackPoint table structure

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    print_first_10_records()