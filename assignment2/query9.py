from tabulate import tabulate
from DbConnector import DbConnector  

def fetch_invalid_activities():
    db_connector = DbConnector()  

    try:
        query = """
        SELECT a.user_id, COUNT(*) AS invalid_activity_count
        FROM Activity a
        JOIN TrackPoint tp1 ON a.id = tp1.activity_id
        JOIN TrackPoint tp2 ON a.id = tp2.activity_id
        WHERE (tp2.date_days - tp1.date_days) >= 0.00347
        AND (tp1.id + 1) = tp2.id  
        GROUP BY a.user_id;
        """
        
        db_connector.cursor.execute(query)
        rows = db_connector.cursor.fetchall()  
        
        if rows:
            print("Users with invalid activities:")
            print(tabulate(rows, headers=["User ID", "Invalid Activity Count"]))
        else:
            print("No invalid activities found in the database.")

    except Exception as e:
        print("ERROR: Failed to fetch invalid activities:", e)

    finally:
        db_connector.close_connection()  

if __name__ == "__main__":
    fetch_invalid_activities()  
