from tabulate import tabulate
from DbConnector import DbConnector

def fetch_top_users():
    db_connector = DbConnector()  
    
    try:
        query = """
        SELECT user_id, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20;
        """
        
        db_connector.cursor.execute(query)
        rows = db_connector.cursor.fetchall()
        
        print("Top 20 users with the highest number of activities:")
        print(tabulate(rows, headers=["User ID", "Activity Count"]))
        
        return rows

    except Exception as e:
        print("ERROR: Failed to fetch top users:", e)

    finally:
        db_connector.close_connection()  

if __name__ == "__main__":
    fetch_top_users()  
