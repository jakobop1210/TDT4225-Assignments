from DbConnector import DbConnector

def count_users_activities_trackpoints():
    """
    Count the total number of users, activities, and trackpoints in the database
    and print the results to the console
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT
            (SELECT COUNT(DISTINCT id) FROM User) AS number_of_users,
            (SELECT COUNT(*) FROM Activity) AS number_of_activities,
            (SELECT COUNT(*) FROM TrackPoint) AS number_of_trackpoints;
    """

    cursor.execute(query)
    query_result = cursor.fetchone()

    if query_result:
        print("Number of users:", query_result[0])
        print("Number of activities:", query_result[1])
        print("Number of trackpoints:", query_result[2])

    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    count_users_activities_trackpoints()
