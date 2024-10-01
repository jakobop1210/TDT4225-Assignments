from DbConnector import DbConnector

## Find all users who have registered transportation_mode and their most used transportation_mode.
## The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
## Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you to decide which transportation mode to include in your answer (choose one).
## Do not count the rows where the mode is null

def execute_query():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = "SELECT user_id, transportation_mode FROM activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode ORDER BY user_id"
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    execute_query()

