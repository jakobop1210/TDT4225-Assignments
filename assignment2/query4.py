from DbConnector import DbConnector

def find_all_user_taken_taxi():
    """
    Find all distinct users that have taken a taxi in the database. 
    Print the user IDs to the console.
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'taxi';
    """
    cursor.execute(query)
    query_result = cursor.fetchall()

    if query_result:
        print("Users that have taken a taxi:")
        for user in query_result:
            print(f"- User ID: {user[0]}")
    else:
        print("No users have taken a taxi")

    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    find_all_user_taken_taxi()
