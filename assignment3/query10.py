from DbConnector import DbConnector

def find_users_in_forbidden_city():
    """
    Find the users who have tracked an activity in the Forbidden City of Beijing.
    The Forbidden City has coordinates approximately: lat 39.916, lon 116.397.
    """
    connection = DbConnector()
    db = connection.db

    # MongoDB aggregation pipeline to replicate SQL logic
    pipeline = [
        {
            "$match": {
                # Match trackpoints with latitude and longitude close to the Forbidden City coordinates
                "lat": { "$gte": 39.916, "$lt": 39.917 },
                "lon": { "$gte": 116.397, "$lt": 116.398 }
            }
        },
        {
            "$lookup": {
                "from": "activities",       
                "localField": "activity_id",   
                "foreignField": "_id",           
                "as": "activity_details"
            }
        },
        {
            "$unwind": "$activity_details"     
        },
        {
            "$group": {
                "_id": "$activity_details.user_id" 
            }
        }
    ]

    # Execute the aggregation pipeline
    result = list(db['trackpoints'].aggregate(pipeline))

    # Output the results
    if result:
        print("Users who have tracked an activity in the Forbidden City:")
        for user in result:
            print(f"- User ID: {user['_id']}")
    else:
        print("No users have tracked an activity in the Forbidden City")

    connection.close_connection()

if __name__ == "__main__":
    find_users_in_forbidden_city()
