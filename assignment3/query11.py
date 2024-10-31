from DbConnector import DbConnector
from tabulate import tabulate

def most_used_transportation_mode():
    """
    Find all users with registered transportation modes and determine their most used mode.
    The result is sorted by user_id and displayed as (user_id, most_used_transportation_mode).
    """
    connection = DbConnector()
    db = connection.db

    try:
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}}, 
            {
                "$group": {
                    "_id": {"user_id": "$user_id", "transportation_mode": "$transportation_mode"},
                    "mode_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id.user_id": 1, "mode_count": -1}  
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_mode": {"$first": "$_id.transportation_mode"},
                    "count": {"$first": "$mode_count"}
                }
            },
            {"$sort": {"_id": 1}}  
        ]

        results = list(db['activities'].aggregate(pipeline))

        print("Users with their most used transportation mode:")
        rows = []
        columns = ["User id", "Transport Mode", "Count"]
        for result in results:
            rows.append([result["_id"],result["most_used_mode"], result["count"]])
        print(tabulate(rows, columns))

    except Exception as e:
        print("ERROR: Failed to fetch most used transportation mode:", e)

    finally:
        connection.close_connection()

if __name__ == "__main__":
    most_used_transportation_mode()
