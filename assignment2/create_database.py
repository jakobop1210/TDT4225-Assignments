from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
import pandas as pd

DATASET_PATH = os.path.join(os.path.dirname(__file__), "dataset", "dataset", "Data")

class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        query1 = """
            CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN
            );
        """

        query2 = """
            CREATE TABLE IF NOT EXISTS Activity (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES User(id)
            );
        """

        query3 = """
            CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            );
        """

        self.cursor.execute(query1)
        self.cursor.execute(query2)
        self.cursor.execute(query3)
        self.db_connection.commit()

    def insert_data(self):
        self.insert_user_data()
        self.insert_activity_data()

    def get_labeled_user_ids(self):
        """
        Find the user IDs that have labels.txt file in the dataset.

        :return: list of user IDs
        """
        labeled_ids_file = 'assignment2/dataset/dataset/labeled_ids.txt'

        with open(labeled_ids_file, 'r', encoding='utf-8') as file:
            labeled_user_ids = file.readlines()

        labeled_user_ids = [line.strip() for line in labeled_user_ids]

        return labeled_user_ids

    def insert_user_data(self):
        """
        Insert user data into the database. This function will insert both labeled and unlabeled user data 
        by looping over all the user folders in the dataset.
        """
        labeled_user_ids = self.get_labeled_user_ids()

        # Use os.walk to traverse directories
        for entry in os.listdir(DATASET_PATH):
            # Skip hidden files
            if entry.startswith('.'):
                continue

            has_labels = entry in labeled_user_ids
            try:
                # Insert into the User table
                self.cursor.execute(
                    "INSERT IGNORE INTO User (ID, has_labels) VALUES (%s, %s)",
                    (entry, has_labels)
                )
            except self.mysql.connector.Error as err:
                print(f"Error: {err}")

        self.db_connection.commit()

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
            return ''

    def insert_activity_data(self):
        """
        Insert activity data into the database. Loops over all the users and their activity files
        and inserts the data into the Activity table. If the user has a labels.txt file, the transportation
        mode will be fetched from the get_transportation_mode function.
        """
        labeled_user_ids = self.get_labeled_user_ids()

        # Get all users from database
        self.cursor.execute("SELECT id FROM User")
        users = self.cursor.fetchall()
        user_ids = [user_id_tuple[0] for user_id_tuple in users]

        for user_id in user_ids:
            # Skip hidden files
            if user_id.startswith('.'):
                continue

            # Path to the folder of the iterated user
            user_folder = os.path.join(DATASET_PATH, user_id)

            # Check if the Trajectory folder exists
            if os.path.exists(f'{user_folder}/Trajectory'):
                activities_path = os.path.join(user_folder, 'Trajectory')

            # Loop over all the activity files in the Trajectory folder
            for activity_file in os.listdir(activities_path):
                activity_file_path = os.path.join(activities_path, activity_file)
                with open(activity_file_path, 'r', encoding='utf-8') as file:
                    rows = file.readlines()

                # Skip hidden files or if rows are more than 2506
                if len(rows) > 2506 or activity_file.startswith('.') or len(rows) < 7:
                    continue
                
                try:
                    # Assign start and end date-time string
                    start_date_time_str = f'{rows[6].strip().split(",")[5]} {rows[6].strip().split(",")[6]}' 
                    end_date_time_str = f'{rows[-1].strip().split(",")[5]} {rows[-1].strip().split(",")[6]}'

                    # Convert to datetime objects
                    start_date_time = datetime.strptime(start_date_time_str, '%Y-%m-%d %H:%M:%S')
                    end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')

                    # Get transportation mode if user is labeled
                    transportation_mode = ""
                    if user_id in labeled_user_ids:
                        transportation_mode = self.get_transportation_mode(user_folder, start_date_time, end_date_time)
        

                    print(f"User: {user_id}, Transportation mode: {transportation_mode}, Start: {start_date_time}, End: {end_date_time}")

                    # Insert the activity into the Activity table in the database
                    self.cursor.execute(
                        "INSERT IGNORE INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)",
                        (user_id, transportation_mode, start_date_time, end_date_time)
                    )

                    # Insert trackpoint data
                    activity_id = self.cursor.lastrowid
                    self.insert_trackpoint_data(activity_id, rows)

                except (ValueError, IndexError) as e:
                    print(f"Error parsing data for {activity_file}: {e}")
                    continue
            self.db_connection.commit()

    def insert_trackpoint_data(self, activity_id, rows):
        """
        Insert trackpoint data into the database. This function will insert trackpoint data
        for a given activity ID and a list of rows from the activity file.
        :param activity_id: ID of the activity
        :param rows: list of rows from the activity file
        """
        for row in rows[6:]: 
            data = row.strip().split(",")
            latitude, longitude, altitude, days, date, time = float(data[0]), float(data[1]), float(data[3]), float(data[4]), data[5], data[6]
            date_time_str = f"{date} {time}"
            date_time = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
            if altitude == '-777':
                altitude = None
            try:
                # Insert each trackpoint into the TrackPoint table
                self.cursor.execute(
                    "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)",
                    (activity_id, latitude, longitude, altitude, days, date_time)
                )
            except Exception as e:
                print(f"Error inserting trackpoint for activity {activity_id}: {e}")
                continue
        print(f"Inserted trackpoint for activity {activity_id}")


    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = Part1()
        program.create_tables()
        program.insert_data()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
