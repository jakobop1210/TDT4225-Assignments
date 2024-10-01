from DbConnector import DbConnector

## Find the top 20 users who have gained the most altitude meters.
## Output should be a table with (id, total meters gained per user).
## Remember that some altitude-values are invalid
## Tip: ∑⬚ ⬚ ⬚ (𝑡𝑝⬚𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝⬚𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝⬚𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > 𝑡𝑝⬚𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑�

def execute_query():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = "SELECT user_id, SUM(altitude) AS total_meters_gained FROM activity WHERE altitude > 0 GROUP BY user_id ORDER BY total_meters_gained DESC LIMIT 20"
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    execute_query()

