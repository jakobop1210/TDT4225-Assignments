from datetime import datetime, timedelta
from pprint import pprint
from DbConnector import DbConnector
import pandas as pd
import os

DATASET_PATH = os.path.join(os.path.dirname(__file__), "dataset", "dataset", "Data")

class CreateDatabase:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)


    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)


    def get_labeled_user_ids(self):
        """
        Find the user IDs that have labels.txt file in the dataset.

        :return: list of user IDs
        """
        labeled_ids_file = os.path.join(os.path.dirname(__file__), "dataset", "dataset", "labeled_ids.txt")

        with open(labeled_ids_file, 'r', encoding='utf-8') as file:
            labeled_user_ids = file.readlines()

        labeled_user_ids = [line.strip() for line in labeled_user_ids]

        return labeled_user_ids


    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()


    def show_collection(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


    def create_tables(self):
        self.create_collection('users')
        self.create_collection('activities')
        self.create_collection('trackpoints')


    def insert_data(self):
        self.insert_users();
        self.insert_activities();


    def insert_users(self):
        labeled_user_ids = self.get_labeled_user_ids()
        users_collection = self.db['users']

        docs = []
        for entry in os.listdir(DATASET_PATH):
            # Skip hidden files
            if entry.startswith('.'):
                continue

            # Prepare document with '_id' as entry name and 'has_labels' as a boolean
            has_labels = entry in labeled_user_ids
            doc = {
                "_id": entry,
                "has_labels": has_labels
            }

            docs.append(doc)

        # Insert all documents at once
        try:
            users_collection.insert_many(docs, ordered=False)
        except Exception as err:
            print(f"Error: {err}")


    def get_transportation_mode(self, user_folder, start_date_time, end_date_time):
        """
        Get the transportation mode for a given user, where start time and end time
        need to match the given parameters, otherwise return an empty string.

        :param user_folder: path to the user folder
        :param start_date_time: start date and time from the activity file
        :param end_date_time: end date and time from the activity file

        :return: transportation mode or empty string
        """
        # Read the labels.txt file
        labels_file_path = os.path.join(user_folder, 'labels.txt')
        labels_file = pd.read_csv(labels_file_path, sep='\t')

        # Convert 'Start Time' and 'End Time' to datetime format
        labels_file['Start Time'] = pd.to_datetime(labels_file['Start Time'], format='%Y/%m/%d %H:%M:%S')
        labels_file['End Time'] = pd.to_datetime(labels_file['End Time'], format='%Y/%m/%d %H:%M:%S')

        # Check if a row matches the given start_date_time and end_date_time
        matching_row = labels_file[(labels_file['Start Time'] == start_date_time) & (labels_file['End Time'] == end_date_time)]

        # If a matching row is found, return the 'Transportation Mode', else return an empty string
        if not matching_row.empty:
            return matching_row.iloc[0]['Transportation Mode']
        else:
            return None


    def insert_activities(self):
        """
        Insert activity data into MongoDB. Loops over all users and their activity files
        and inserts the data into the specified collection. If the user has a labels.txt file,
        the transportation mode will be fetched from get_transportation_mode.
        """
        labeled_user_ids = self.get_labeled_user_ids()
        activities_collection = self.db['activities']
        users_collection = self.db['users']

        # Get all users from the 'users' collection
        users = users_collection.find({}, {"_id": 1})
        user_ids = [user["_id"] for user in users]

        for user_id in user_ids:
            # Skip hidden files
            if user_id.startswith('.'):
                continue

            # Path to the folder of the iterated user
            user_folder = os.path.join(DATASET_PATH, user_id)

            # Check if the Trajectory folder exists
            activities_path = os.path.join(user_folder, 'Trajectory')
            if not os.path.exists(activities_path):
                continue

            # Loop over all the activity files in the Trajectory folder
            for activity_file in os.listdir(activities_path):
                activity_file_path = os.path.join(activities_path, activity_file)
                with open(activity_file_path, 'r', encoding='utf-8') as file:
                    rows = file.readlines()

                # Skip hidden files or files with rows more than 2506 or fewer than 7
                if len(rows) > 2506 or activity_file.startswith('.') or len(rows) < 7:
                    continue

                try:
                    # Assign start and end date-time string
                    start_date_time_str = f"{rows[6].strip().split(',')[5]} {rows[6].strip().split(',')[6]}"
                    end_date_time_str = f"{rows[-1].strip().split(',')[5]} {rows[-1].strip().split(',')[6]}"

                    # Convert to datetime objects
                    start_date_time = datetime.strptime(start_date_time_str, '%Y-%m-%d %H:%M:%S')
                    end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')

                    # Get transportation mode if user is labeled
                    transportation_mode = None
                    if user_id in labeled_user_ids:
                        transportation_mode = self.get_transportation_mode(user_folder, start_date_time, end_date_time)

                    # Determine if the activity is valid
                    is_valid = self.validate_activity(rows)

                    # Create activity document
                    activity_doc = {
                        "user_id": user_id,
                        "transportation_mode": transportation_mode,
                        "start_date_time": start_date_time,
                        "end_date_time": end_date_time,
                        "is_valid": is_valid,
                    }

                    inserted_activity = activities_collection.insert_one(activity_doc)
                    activity_id = inserted_activity.inserted_id  # Get the ID of the inserted document

                    # Insert trackpoint data using the new activity_id
                    self.insert_trackpoint_data(activity_id, rows)

                except (ValueError, IndexError) as e:
                    print(f"Error parsing data for {activity_file}: {e}")
                    continue


    def validate_activity(self, rows):
        """
        Validate the activity by checking if any consecutive trackpoints have timestamps
        that deviate by at least 5 minutes. If such a deviation exists, the activity is invalid.
        """
        timestamps = []

        # Extract timestamps from rows
        for row in rows[7:]:
            try:
                # Extract the timestamp
                timestamp_str = row.strip().split(',')[5] + ' ' + row.strip().split(',')[6]
                timestamps.append(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S'))
            except (ValueError, IndexError):
                print(f"Error parsing timestamp from row: {row}")
                return False  # If there's an error in parsing, consider the activity invalid

        # Check for deviations in timestamps
        for i in range(1, len(timestamps)):
            time_difference = (timestamps[i] - timestamps[i - 1]).total_seconds() / 60  # Convert to minutes
            if time_difference >= 5:
                return False  # Invalid if any pair of consecutive timestamps deviate by 5 minutes

        return True


    def check_invalid_activity(self, timestamps):
        # Check for any consecutive trackpoints that deviate by at least 5 minutes
        for i in range(1, len(timestamps)):
            if (timestamps[i] - timestamps[i - 1]) >= timedelta(minutes=5):
                return True  # Mark as invalid if the difference is 5 minutes or more
        return False  # Otherwise, valid


    def insert_trackpoint_data(self, activity_id, rows):
        activity_doc = self.db['activities'].find_one({"_id": activity_id})

        if not activity_doc:
            print(f"Activity with ID {activity_id} not found.")
            return

        trackpoints = []
        global prev_alt
        prev_alt = None
        
        for row in rows[6:]:
            data = row.strip().split(",")

            # Extract data from the row
            latitude = float(data[0])
            longitude = float(data[1])
            altitude = float(data[3])
            days = float(data[4])
            date = data[5]
            time = data[6]
            date_time_str = f"{date} {time}"
            date_time = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')

            # Handle the altitude value
            if altitude == -777:
                altitude = None

            alt_inc = self.get_alt_inc(altitude)

            # Create a trackpoint document
            trackpoint = {
                "activity_id": activity_id,
                "lat": latitude,
                "lon": longitude,
                "altitude": altitude,
                "date_days": days,
                "date_time": date_time,
                "alt_inc": alt_inc
            }
            trackpoints.append(trackpoint)

        # Insert all trackpoints into the 'trackpoints' collection
        try:
            if trackpoints:
                self.db['trackpoints'].insert_many(trackpoints)
                print(f"Inserted {len(trackpoints)} trackpoints for activity {activity_id}")
        except Exception as e:
            print(f"Error inserting trackpoints for activity {activity_id}: {e}")


    def drop_all_tables(self):
        print("Drop whole database...")
        self.db.drop_collection('users')
        self.db.drop_collection('activities')
        self.db.drop_collection('trackpoints')


    def get_alt_inc(self, alt):
        global prev_alt
        if alt == None:
            return 0
        if prev_alt == None:
            result = 0
        else:
            result = alt - prev_alt
        prev_alt = alt
        return result


    def print_first_2_documents(self):
        """
        Print the first 2 documents of each collection.
        """
        collections = ["users", "activities", "trackpoints"]

        for collection_name in collections:
            collection = self.db[collection_name]
            print(f"\nFirst 2 rows in collection '{collection_name}':")

            # Fetch the first 2 documents and print them
            documents = collection.find().sort("_id", 1).limit(2)
            for doc in documents:
                pprint(doc)


def main():
    program = None
    try:
        program = CreateDatabase()

        program.drop_all_tables()
        program.create_tables()
        program.insert_data()
        program.print_first_2_documents()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
