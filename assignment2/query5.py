from tabulate import tabulate
from DbConnector import DbConnector

def find_activities_for_all_modes():
    """
    Find all types of transportation modes and count how many activities that are tagged
    with these transportation mode labels. Do not count the rows where the mode is null.
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT transportation_mode, COUNT(transportation_mode)
        FROM Activity
        WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode
    """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        table_data = []
        for mode, count in result:
            table_data.append([mode, count])

        print(tabulate(table_data, headers=["Transportation Mode", "Activity Count"], tablefmt="grid"))
    else:
        print("No transportation modes found")

    db_connection.close()
    connection.close_connection()

if __name__ == '__main__':
    find_activities_for_all_modes()

