from DbConnector import DbConnector

## Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.

def execute_query():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = "SELECT transportation_mode, COUNT(transportation_mode) FROM activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode"
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    execute_query()

