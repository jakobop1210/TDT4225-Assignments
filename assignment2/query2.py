from DbConnector import DbConnector

def average_activities():
    """
    Find the average number of activities per user.
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT COUNT(id) / COUNT(DISTINCT user_id) AS average_activities_per_user
        FROM Activity
    """

    cursor.execute(query)
    query_result = cursor.fetchall()

    if query_result:
        print("Average number of activities per user:")
        print(f"- {query_result[0][0]:.2f}")
    else:
        print("No activities found")

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    average_activities()
