from DbConnector import DbConnector
import math

def total_distance_id112():
    """
    Find the total distance (in km) walked in 2008, by user with id=112
    """
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    total_distance = 0

    walking_activities = """
        SELECT id AS activity_id
        FROM Activity
        WHERE user_id = 112
        AND transportation_mode = 'walk'
        AND YEAR(start_date_time) = 2008;
    """

    cursor.execute(walking_activities)
    activities = cursor.fetchall()

    for activity in activities:
        activity_id = activity[0]

        trackpoint_query = """
            SELECT lat, lon
            FROM TrackPoint
            WHERE activity_id = %s
            ORDER BY date_time ASC;
        """
        cursor.execute(trackpoint_query, (activity_id,))
        trackpoints = cursor.fetchall()

        for i in range(1, len(trackpoints)):
            lat1, lon1 = trackpoints[i-1]
            lat2, lon2 = trackpoints[i]
            distance = haversine(lat1, lon1, lat2, lon2)
            total_distance += distance

    print(f"Total distance walked by user with id=112 in 2008: {total_distance:.2f} km")

    db_connection.close()
    connection.close_connection()



def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points. 
    on the earth (specified in decimal degrees)

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