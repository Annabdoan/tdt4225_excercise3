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
        query = [
            {
                "$group": {
                    "_id": "$user_id",
                    "activity_count": {"$sum": 1}
                }
            },
            {"$sort": {"activity_count": -1}},
            {"$limit": 20}
        ]

        top_20_users = list(self.db.Activity.aggregate(query))

        print("The top 20 users with the highest number of activities:")
        for idx, user in enumerate(top_20_users, 1):
            pprint({
                "Rank": idx,
                "User ID": user["_id"],
                "Number of Activities": user["activity_count"]
            })

        return top_20_users

    
    def query4(self):
    # All users who have taken a taxi
        taxi_users = self.db.Activity.distinct("user_id", {"transportation_mode": "taxi"})

        print("Users who have taken a taxi:")
        for user_id in taxi_users:
            pprint({
                "User ID": user_id
            })

        return taxi_users

    
    def query5(self):
    # All types of transportation modes and count how many activities that are tagged with these transportation mode labels
    # Do not count the rows where the mode is null
        query = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None} # Exclude where mode is null
                    "user_id": {"$in": self.db.User.distinct("_id", {"has_labels": True})} #only users with labels
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]

        transportation_modes = list(self.db.Activity.aggregate(query))

        print("All types of transportation modes and their count:")
        for transmode in transportation_modes:
            pprint({
                "Transportation Mode": transmode["_id"],
                "Count": transmode["count"]
            })

        return transportation_modes


    def query6(self):
    # a) The year with the most activities
        query_most_activities = [
            {
                "$group": {
                    "_id": {"$year": "$end_date_time"},
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"activity_count": -1}
            },
            {
                "$limit": 1
            }
        ]
        year_most_activities = list(self.db.Activity.aggregate(query_most_activities))[0]

    # b) Is this also the year with the most recorded hours?
        query_most_hours = [
            {
                "$project": {
                    "year": {"$year": "$end_date_time"},
                    "activity_hours": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}
                }
            },
            {
                "$group": {
                    "_id": "$year",
                    "total_hours": {"$sum": "$activity_hours"}
                }
            },
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ]
        year_most_hours = list(self.db.Activity.aggregate(query_most_hours))[0]

        # Compare year most activities and year most hours
        if year_most_activities["_id"] == year_most_hours["_id"]:
            print(f"The year {year_most_activities['_id']} has the most activities and the most recorded hours.")
        else:
            print(f"The year {year_most_activities['_id']} has the most activities, but the year {year_most_hours['_id']} has the most recorded hours.")

        pprint({
            "Year with most activities": year_most_activities["_id"],
        })
        pprint({
            "Year with most recorded hours": year_most_hours["_id"],
        })

        return year_most_activities, year_most_hours


    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Radius of earth in km
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c
    
    def query7(self):
    # Total distance (in km) walked in 2008 by user with id=112
    # The total distance walked is the sum of the distances between consecutive trackpoints
        walking_activities = self.db.Activity.find({
            "user_id": 112,
            "transportation_mode": "walk",
            "start_date_time": {"$gte": "2008-01-01", "$lt": "2009-01-01"}
        }, {"_id": 1})

        total_distance = 0

        for activity in walking_activities:
            activity_id = activity["_id"]

            # Gwt the trackpoints for the activity
            trackpoints = list(self.db.TrackPoint.find(
                {"activity_id": activity_id},
                {"lat": 1, "lon": 1}
            ).sort("date_time", 1))

            # Calculate the distance between consecutive trackpoints
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"]
                lat2, lon2 = trackpoints[i]["lat"], trackpoints[i]["lon"]
                total_distance += self.haversine(lat1, lon1, lat2, lon2)

        pprint({
            "User ID": 112,
            "Year": 2008,
            "Total Distance Walked (km)": f"{total_distance:.2f} km"
        })

        return total_distance
    

    def query8(self):
    # Find the top 20 users who have gained the most altitude meters
    # Output should be a table with (id, total meters gained per user)
    # Remember that some altitude meters are invalid 
    # Tip: math formula (look at task sheet)¨
    # Note: Remember that we are looking for altitude GAIN 
        query = [
            {
                "$lookup": {
                    "from": "TrackPoint",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "trackpoints"
                }
            },
            {
                "$unwind": "$trackpoints"
            },
            {
                "$match": {
                    "trackpoints.altitude": {"$ne": -777} #invalid alt
                }
            },
            {
                "$group": {
                    "_id": {"user_id": "$user_id", "activity_id": "$_id"},
                    "trackpoints": {"$push": "$trackpoints"}
                }
            }
        ]

        valid_activities = list(self.db.Activity.aggregate(pipeline))

        alt_gain_per_user = {}

        for activitiy in valid_activities:
            user_id = activity["_id"]["user_id"]
            trackpoints = activity["trackpoints"]

            # at least 2 trackpoints to calculate gain in alt
            if len(trackpoints) < 2:
                continue

            activity_total_gain = 0
            for i in range(1, len(trackpoints)):
                curr_alt = trackpoints[i]["altitude"]
                prev_alt = trackpoints[i - 1]["altitude"]

                if curr_alt > prev_alt:
                    activity_total_gain += (current_alt - previous_altitude)

            if user_id in alt_gain_per_user:
                alt_gain_per_user[user_id] += activity_total_gain
            else:
                alt_gain_per_user[user_id] = activity_total_gain
        
        # Sort users by total altitude gain and get the top 20
        top_20_users = sorted(alt_gain_per_user.items(), key=lambda x: x[1], reverse=True)[:20]

        # Format the results as a table using tabulate and print it
        table_data = [(user_id, f"{total_gain:.2f} meters") for user_id, total_gain in top_20_users]
        table = tabulate(table_data, headers=["User ID", "Total Altitude Gain"], tablefmt="pretty")

        print("Top 20 users with the most altitude gain:")
        print(table)

        # Pretty print the result
        pprint({"Top 20 Users by Altitude Gain": top_20_users})

        return top_20_users
        


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