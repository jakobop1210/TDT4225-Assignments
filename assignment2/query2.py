from DbConnector import DbConnector

## Find the average number of activities per user.

def execute_query():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = "SELECT COUNT(activity_id) / COUNT(DISTINCT user_id) AS average_activities_per_user FROM activity"
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    execute_query()
