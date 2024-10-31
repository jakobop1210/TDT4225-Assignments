from DbConnector import DbConnector
from tabulate import tabulate

def users_most_gained_elevation():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.trackpoints.aggregate(
        [
            {"$match": {"alt_inc": {"$gt": 0}}},  
            {
                "$lookup": {  
                    "from": "activities",
                    "localField": "activity_id",
                    "foreignField": "_id",
                    "as": "activity_info"
                }
            },
            {"$unwind": "$activity_info"}, 
            {
                "$group": {
                    "_id": "$activity_info.user_id",  
                    "total_alt": {"$sum": "$alt_inc"}  
                }
            },
            {"$sort": {"total_alt": -1}},  
            {"$limit": 20}  
        ]
    )

    if query:
        print("Top 20 users with the highest altitude gains: ")
        rows = []
        for i, doc in enumerate(query):
            rows.append([i + 1, doc["_id"], doc["total_alt"]*0.3048])
        columns = ["Rank", "User ID", "Altitude Meters Gained"]
        print(tabulate(rows, columns))
    else:
        print("Something went wrong")

    client.close()

if __name__ == "__main__":
    users_most_gained_elevation()
