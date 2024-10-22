from DbConnector import DbConnector
from tabulate import tabulate 
from math import radians, sin, cos, sqrt, atan2
from pprint import pprint

class ProgramQueries:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


    def query1(self):
    ## How many users, activities and trackpoints there are in the dataset
    ## TODO å teste disse
        query_users = self.db.User.count_documents({})
        # query_activity = self.db.Activity.find().count()
        # query_trackpoint = self.db.Trackpoint.find().count()

        pprint("Number of users".format(query_users))
        # print("Number of activities".format(query_activity))
        # print("Number of trackpoints".format(query_trackpoint))
        
    
    def query2(self):
    # Find the average number of activities per user
        
        activity_count_pr_user = self.db.get_collection('Activity').aggregate([
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        ])

        total_activities = sum([doc["activity_count"] for doc in activity_count_pr_user])
        avg_activities_per_user = total_activities / len(activity_count_pr_user)
        
        pprint("Average number of activities per user: {:.2f}".format(avg_activities_per_user))


    def query3(self):
    # The top 20 users with the highest number of activities

        return 

    
    def query4(self):
    # All users who have taken a taxi

        return 

    
    def query5(self):
    # All types of transportation modes and count how many activities that are tagged with these transportation mode labels
    # Do not count the rows where the mode is null
        return 


    def query6(self):
    # a) The year with the most activities
    # b) Is this also the year with the most recorded hours?
        return 


    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Radius of the Earth in km
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c
    
    def query7(self):
    # Total distance (in km) walked in 2008 by user with id=112
    # The total distance walked is the sum of the distances between consecutive trackpoints
        
        return 
    

    def query8(self):
    # Find the top 20 users who have gained the most altitude meters
    # Output should be a table with (id, total meters gained per user)
    # Remember that some altitude meters are invalid 
    # Tip: math formula (look at task sheet)¨
    # Note: Remember that we are looking for altitude GAIN 

        return 
        


    def query9(self):
    # All users who have invalid activities, and the number of invalid activities per user
    # An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes

        return 


    def query10(self):
    # Users who have tracked an activity in the Forbidden City of Beijing
    # Forbidden City of Beijing coordinates: lat 39.916, lon 116.397
    # Find trackpoints tracked in the forbidden city

        return 

    
    def query11(self):
    # All users who have registered transportation_mode and their most used transportation_mode
    # The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id
    # Some users may have the same number of activities tagged with e.g. walk and car. 
    # In this case it is up to you to decide which transportation mode to include in your answer (choose one)
    # Do not count the rows where the mode is null

        return
    
    def close_connection(self):
        self.connection.close_connection()
            
def main():
    program = None
    try:
        program = ProgramQueries()
        # program.query1()
        # program.query2()
        # program.query3()
        # program.query4()
        # program.query5()
        # program.query6()
        # program.query7()
        # program.query8()
        # program.query9()
        # program.query10()
        # program.query11()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.close_connection()

if __name__ == "__main__":
    main()