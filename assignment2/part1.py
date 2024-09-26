from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime



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
                id INT NOT NULL PRIMARY KEY,
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
        self.insert_trackpoint_data()

    def insert_user_data(self):
        labeled_ids_file = 'assignment2/dataset/dataset/labeled_ids.txt'

        with open(labeled_ids_file, 'r', encoding='utf-8') as file:
            labeled_user_ids = file.readlines()

        labeled_user_ids = [line.strip() for line in labeled_user_ids]

        data_folder = 'assignment2/dataset/dataset/Data'

        # Use os.walk to traverse directories
        for entry in os.listdir(data_folder):
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


    def insert_activity_data(self):
        # Loop through each user ID folder
        data_folder = 'assignment2/dataset/dataset/Data'
        for user_id in os.listdir(data_folder):
            # Skip hidden files
            if user_id.startswith('.'):
                continue

            user_folder = f'assignment2/dataset/dataset/Data/{user_id}/Trajectory'

            for activity_file in os.listdir(user_folder):
                # Open and read the file, skipping files with too many rows
                activity_file_path = f'{user_folder}/{activity_file}'
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

                    # Extract the activity id from the file name
                    activity_id = int(activity_file.split(".")[0])

                    print(f"Inserting activity {activity_id} for user {user_id}")

                    # Insert the activity into the Activity table in the database
                    self.cursor.execute(
                        "INSERT IGNORE INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)",
                        (activity_id, user_id, False, start_date_time, end_date_time)
                    )
                    self.db_connection.commit()
                except (ValueError, IndexError) as e:
                    print(f"Error parsing data for {activity_file}: {e}")
                    continue  # Skip if there's an error in parsing

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

    def insert_trackpoint_data(self):
        data_folder = 'assignment2/dataset/dataset/Data'

        # loop through user folders
        for entry in os.listdir(data_folder):
            entry_path = os.path.join(data_folder, entry)

            if os.path.isdir(entry_path):
                try:
                    # Insert into the User table
                    self.cursor.execute(
                        "INSERT IGNORE INTO User (ID, has_labels) VALUES (%s, %s)",
                        (entry, 0)  # Assuming has_labels is 0 for now, adjust based on your data
                    )
                except self.mysql.connector.Error as err:
                    print(f"Error: {err}")

                # Navigate into the Trajectory folder for each user
                trajectory_folder = os.path.join(entry_path, 'Trajectory')
                if os.path.exists(trajectory_folder):

                    # loop through activity files
                    for plt_file in os.listdir(trajectory_folder):
                        plt_path = os.path.join(trajectory_folder, plt_file)

                        try:
                            # Check the number of lines in the activity file
                            with open(plt_path, 'r') as file:
                                line_count = sum(1 for _ in file)

                            # Skip the file if it has more than 2506 lines
                            if line_count > 2506:
                                print(f"Skipping {plt_path}: contains {line_count} lines.")
                                continue

                            # Open and read the activity file
                            with open(plt_path, 'r') as file:
                                # Skip the first 6 lines (headers or metadata)
                                for _ in range(6):
                                    next(file)
                                for line in file:
                                    parts = line.strip().split(',')
                                    # Extract the necessary fields (assuming: latitude, longitude, etc.)
                                    lat, lon, altitude = parts[0], parts[1], parts[3]
                                    date_days, date, time = parts[4], parts[5], parts[6]

                                    date_time = f"{date} {time}"  # Combine date and time

                                    # Get activity_id from the filename
                                    #activity_id = int(os.path.splitext(plt_file)[0])

                                    # Insert trackpoint data into the database (adjust query as needed)
                                    self.cursor.execute(
                                        "INSERT INTO TrackPoint (lat, lon, altitude, date_days, date_time) "
                                        "VALUES (%s, %s, %s, %s, %s)",
                                        (lat, lon, altitude, date_days, date_time)
                                    )
                        except Exception as e:
                            print(f"Failed to read {plt_path}: {e}")

        self.db_connection.commit()


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

        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
