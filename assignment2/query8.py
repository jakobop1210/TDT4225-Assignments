from DbConnector import DbConnector
from tabulate import tabulate

def find_users_with_most_altitudes():
    """
    Find the top 20 users who have gained the most altitude meters.
    Output should be a table with (id, total meters gained per user).
    Remember that some altitude-values are invalid
    Tip: ∑⬚ ⬚ ⬚ (𝑡𝑝⬚𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝⬚𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝⬚𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > 𝑡𝑝⬚𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑�
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT Activity.user_id, SUM(GREATEST(0, t1.altitude - t2.altitude)) AS total_feet_gained
        FROM TrackPoint AS t1
        JOIN TrackPoint AS t2
            ON t1.activity_id = t2.activity_id
            AND t1.id = (t2.id + 1)
        JOIN Activity ON t1.activity_id = Activity.id
        WHERE t1.altitude IS NOT NULL
        AND t2.altitude IS NOT NULL
        GROUP BY Activity.user_id
        ORDER BY total_feet_gained DESC
        LIMIT 20
    """
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        print("Top 20 Users by Altitude Gained:")
        print(tabulate(result, headers=["User ID", "Total Feet Gained"], tablefmt="fancy_grid"))
    else:
        print("No users found")

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    find_users_with_most_altitudes()

