from DbConnector import DbConnector
from tabulate import tabulate

def users_most_gained_elevation():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.trackpoints.aggregate(
        [
            {"$match": {"alt_inc": {"$gt": 0}}},  # Only positive altitude increments
            {
                "$lookup": {  # Join with activities to get user_id
                    "from": "activities",
                    "localField": "activity_id",
                    "foreignField": "_id",
                    "as": "activity_info"
                }
            },
            {"$unwind": "$activity_info"},  # Flatten the joined activity info
            {
                "$group": {
                    "_id": "$activity_info.user_id",  # Group by user_id from activity_info
                    "total_alt": {"$sum": "$alt_inc"}  # Sum of altitude increments
                }
            },
            {"$sort": {"total_alt": -1}},  # Sort by total altitude descending
            {"$limit": 20}  # Limit to top 20 users
        ]
    )

    if query:
        print("Top 20 users with the most altitude meters: ")
        rows = []
        for i, doc in enumerate(query):
            rows.append([i + 1, doc["_id"], doc["total_alt"]*0.3048])
        columns = ["Rank", "User ID", "Total Altitude Meters"]
        print(tabulate(rows, columns))
    else:
        print("Something went wrong")

    client.close()

if __name__ == "__main__":
    users_most_gained_elevation()
