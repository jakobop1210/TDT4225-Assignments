from DbConnector import DbConnector
from tabulate import tabulate
import os

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
            CREATE TABLE IF NOT EXISTS TrackPoints (
                id INT NOT NULL PRIMARY KEY,
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

        """ names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit() """

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
