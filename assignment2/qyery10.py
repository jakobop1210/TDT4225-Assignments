from DbConnector import DbConnector

def find_users_in_forbidden_city():
    """
    Find the users who have tracked an activity in the Forbidden City of Beijing.
    The Forbidden City have coordinates that correspond to: lat 39.916, lon 116.397.
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE id IN (
            SELECT activity_id
            FROM TrackPoint
            WHERE CAST(TrackPoint.lat AS CHAR) LIKE '39.916%'
            AND CAST(TrackPoint.lon AS CHAR) LIKE '116.397%'
        );
    """

    cursor.execute(query)
    query_result = cursor.fetchall()

    if query_result:
        print("Users who have tracked an activity in the Forbidden City:")
        for user in query_result:
            print(f"- User ID: {user[0]}")
    else:
        print("No users have tracked an activity in the Forbidden City")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    find_users_in_forbidden_city()
