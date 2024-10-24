from pprint import pprint
from DbConnector import DbConnector
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

def main():
    program = None
    try:
        program = CreateDatabase()
        program.drop_all_tables()
        program.create_tables()
        program.insert_data()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
