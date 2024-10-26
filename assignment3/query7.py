from datetime import datetime
from pprint import pprint
from DbConnector import DbConnector
import math

def total_distance_id112():
    """
    Find the total distance (in km) walked in 2008 by user with id=112.
    First, find all activities for user with id=112 in 2008 where the transportation mode is 'walk'.
    Then find all trackpoints for each activity.
    Calculate the distance between each pair of trackpoints and sum them all up.
    """
    db_connector = DbConnector()
    db = db_connector.db

    total_distance = 0

    # Define the date range
    start_date = datetime(2008, 1, 1)
    end_date = datetime(2009, 1, 1)

    # Find all walking activities for user_id 112 in the year 2008
    walking_activities = list(db['activities'].find({
        "user_id": "112",
        "transportation_mode": "walk",
        "start_date_time": {
            "$gte": start_date,
            "$lt": end_date
        }
    }))

    for activity in walking_activities:
        # Find all trackpoints for each activity, ordered by date_time
        trackpoints = list(db['trackpoints'].find({
            "activity_id": activity['_id']
        }).sort("date_time", 1))

        # Calculate distance between consecutive trackpoints
        for i in range(1, len(trackpoints)):
            lat1, lon1 = trackpoints[i-1]['lat'], trackpoints[i-1]['lon']
            lat2, lon2 = trackpoints[i]['lat'], trackpoints[i]['lon']
            distance = haversine(lat1, lon1, lat2, lon2)
            total_distance += distance

    output = {
        "Total distance walked by user with id=112 in 2008 (km)": total_distance
    }
    
    pprint(output)

    db_connector.close_connection()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).

    Parameters:
    lat1 (float): Latitude of point 1
    lon1 (float): Longitude of point 1
    lat2 (float): Latitude of point 2
    lon2 (float): Longitude of point 2

    Returns: float: The distance between the two points in kilometers
    """
    R = 6371  # Radius of the Earth in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

if __name__ == "__main__":
    total_distance_id112()
