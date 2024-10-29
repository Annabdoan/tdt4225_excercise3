from pprint import pprint 
from DbConnector import DbConnector
from dataextracter import *
import bson

class ExampleProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


    def insert_many_documents(self, collection_name, insert_data):
        collection = self.db[collection_name]
        collection.insert_many(insert_data)
        print(f"Inserted {len(insert_data)} documents into collection {collection_name}")         


def main():
    program = None
    try:
        program = ExampleProgram()
        program.drop_coll(collection_name='User')
        program.drop_coll(collection_name='Activity')
        program.drop_coll(collection_name='Trackpoint')
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="Trackpoint")
        program.show_coll()

        users = extract_data()
        users = set_has_labels(users)
        users = add_labels(users)

        # Prepare lists to store documents
        trackpoint_documents = []
        activity_documents = []
        user_documents = []

        max_batch_size = 16 * 1024 * 1024  # 16MB, maximum BSON size in MongoDB
        current_batch_size = 0

        for user in users:
            if not isinstance(int(user.id), int):
                raise TypeError(f"ID must be an integer, not a " + str(type(user.id)))
            if not isinstance(user.has_labels, bool):
                raise TypeError("has_labels must be a boolean, not a " + str(type(user.has_labels)))
            user_data = {
                "_id": int(user.id),  # Custom ID
                "has_labels": user.has_labels,
                "activity_ids": []
            }

            for activity in user.activities:
                if not isinstance(int(activity.id), int):
                    raise TypeError("Activity ID must be an integer, not a " + str(type(activity.id)))
                if not isinstance(int(activity.user_id), int):
                    raise TypeError("Activity_ID must be an int, not a " + str(type(activity.user_id)))
                if not isinstance(activity.start_date_time, datetime):
                    raise TypeError("Activity start_date_time must be a datetime object, not " + str(type(activity.start_date_time)))
                if not isinstance(activity.end_date_time, datetime):
                    raise TypeError("Activity end date time must be a datetime object, not " + str(type(activity.end_date_time)))
                
                # Use activity's assigned ID to track point insertion
                activity_id = activity.id  # Ensure trackpoints reference this specific activity ID
                activity_data = {
                    "_id": activity_id,  # Custom ID
                    "user_id": int(activity.user_id),
                    "transportation_mode": activity.transportation_mode,
                    "start_date_time": activity.start_date_time,
                    "end_date_time": activity.end_date_time,
                    "trackpoint_ids": []
                }

                for trackpoint in activity.trackpoints:
                    if not isinstance(int(trackpoint.ID), int):
                        raise TypeError("ID must be an integer, not " + str(type(trackpoint.ID)))
                    if not isinstance(float(trackpoint.lat), float):
                        raise TypeError("Latitude must be a double (float), not " + str(type(trackpoint.lat)))
                    if not isinstance(float(trackpoint.lon), float):
                        raise TypeError("Longitude must be a double (float), not " + str(type(trackpoint.lon)))
                    if not isinstance(round(float(trackpoint.altitude)), int):
                        raise TypeError("Altitude must be an integer, not " + str(type(trackpoint.altitude)))
                    if not isinstance(trackpoint.date_time, datetime):
                        raise TypeError("Trackpoint date time must be a datetime object, not " + str(type(trackpoint.date_time)))
                    
                    trackpoint_data = {
                        "_id": int(trackpoint.ID),  # Custom ID
                        "activity_id": activity_id,  # Ensure this trackpoint refers to the correct activity
                        "lat": float(trackpoint.lat),
                        "lon": float(trackpoint.lon),
                        "altitude": round(float(trackpoint.altitude)),
                        "date_time": trackpoint.date_time
                    }
                    # Calculate size of the document
                    trackpoint_size = len(bson.BSON.encode(trackpoint_data))

                    # Check if adding this document exceeds max_batch_size
                    if current_batch_size + trackpoint_size > max_batch_size:
                        # Insert the current batch and reset
                        program.insert_many_documents(collection_name="Trackpoint", insert_data=trackpoint_documents)
                        trackpoint_documents = []
                        current_batch_size = 0

                    trackpoint_documents.append(trackpoint_data)
                    current_batch_size += trackpoint_size
                    activity_data["trackpoint_ids"].append(trackpoint_data["_id"])

                # Calculate size of the activity document
                activity_size = len(bson.BSON.encode(activity_data))

                # Check if adding this document exceeds max_batch_size
                if current_batch_size + activity_size > max_batch_size:
                    # Insert the current batch and reset
                    program.insert_many_documents(collection_name="Activity", insert_data=activity_documents)
                    activity_documents = []
                    current_batch_size = 0

                activity_documents.append(activity_data)
                user_data["activity_ids"].append(activity_data["_id"])

            # Calculate size of the user document
            user_size = len(bson.BSON.encode(user_data))

            # Check if adding this document exceeds max_batch_size
            if current_batch_size + user_size > max_batch_size:
                # Insert the current batch and reset
                program.insert_many_documents(collection_name="User", insert_data=user_documents)
                user_documents = []
                current_batch_size = 0

            user_documents.append(user_data)
            current_batch_size += user_size

        # Insert any remaining documents in the last batch
        if trackpoint_documents:
            program.insert_many_documents(collection_name="Trackpoint", insert_data=trackpoint_documents)

        if activity_documents:
            program.insert_many_documents(collection_name="Activity", insert_data=activity_documents)

        if user_documents:
            program.insert_many_documents(collection_name="User", insert_data=user_documents)

        program.show_coll()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if program:
            program.connection.close_connection()



if __name__ == '__main__':
    main()
