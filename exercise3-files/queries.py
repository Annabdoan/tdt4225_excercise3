from DbConnector import DbConnector
from tabulate import tabulate 
from math import radians, sin, cos, sqrt, atan2
from pprint import pprint
from datetime import datetime

class ProgramQueries:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


    def query1(self):
    ## How many users, activities and trackpoints there are in the dataset
        query_users = self.db.User.count_documents({})
        query_activity = self.db.Activity.count_documents({})
        query_trackpoints = self.db.Trackpoint.count_documents({})
        
        pprint(f"Number of users: {query_users}")
        pprint(f"Number of activities: {query_activity}")
        pprint(f"Number of trackpoints: {query_trackpoints}")
       
    
    def query2(self):
        # Find the average number of activities per user
        query = [
            {
                "$group": {
                    "_id": "$user_id",  # Group by user_id
                    "activities_per_user": { "$sum": 1 }  # Count the number of activities per user
                }
            },
            {
                "$group": {
                    "_id": None,
                    "average_activities_per_user": { "$avg": "$activities_per_user" }  # Calculate average
                }
            }
        ]

        # Run the aggregation query
        result = self.db.Activity.aggregate(query)

        # Retrieve and print the result
        for item in result:
            pprint(f"Average number of activities per user: {item['average_activities_per_user']:.2f}")




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
    # # Do not count the rows where the mode is null
        query = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None}, # Exclude where mode is null
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


    # Define haversine function, could not get haversine package to work
    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Radius of earth in km
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c
    
    def query7(self):
        print("Starting query7 to calculate total walking distance for user 112 in 2008...")

        # Step 1: All walking activities for user 112 in 2008
        walking_activities = self.db.Activity.find({
            "user_id": 112,
            "transportation_mode": "walk",
            "start_date_time": {"$gte": datetime(2008, 1, 1), "$lt": datetime(2009, 1, 1)}
        }, {"_id": 1})

        total_distance = 0
        activity_count = 0  

        print("Walking activities found:")
        walking_activity_ids = [activity["_id"] for activity in walking_activities]  # Collect activity IDs for debugging
        print(walking_activity_ids)

        for activity_id in walking_activity_ids:
            print(f"\nProcessing Activity ID: {activity_id}")
            activity_count += 1 

            # Step 2: Retrieve trackpoints for each activity
            trackpoints = list(self.db.Trackpoint.find(
                {"activity_id": activity_id},
                {"lat": 1, "lon": 1, "date_time": 1}
            ).sort("date_time", 1))

            if not trackpoints:
                print(f"  No trackpoints found for Activity ID: {activity_id}")
                continue

            print(f"  Number of trackpoints found: {len(trackpoints)}")

            # Step 3: Must be more than two trackpoints to calculate distance
            if len(trackpoints) < 2:
                print(f"  Skipping Activity ID {activity_id}: Less than 2 trackpoints.")
                continue

            # Step 4: Find distances between consecutive trackpoints
            activity_distance = 0  # Total distance for this activity
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"]
                lat2, lon2 = trackpoints[i]["lat"], trackpoints[i]["lon"]
                distance = self.haversine(lat1, lon1, lat2, lon2)
                activity_distance += distance
                print(f"    Distance between point {i-1} and {i}: {distance:.4f} km")

            print(f"  Total distance for Activity ID {activity_id}: {activity_distance:.4f} km")
            total_distance += activity_distance 

        # Step 5: Print and return the total distance walked in 2008 by user 112
        print(f"\nTotal walking activities processed for user 112: {activity_count}")
        pprint({
            "User ID": 112,
            "Year": 2008,
            "Total Distance Walked (km)": f"{total_distance:.2f} km"
        })

        return total_distance
    

    def query8(self):
        print("Running query...")

        query = [
            {
                "$lookup": {
                    "from": "Trackpoint",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "trackpoints"
                }
            },
            {
                "$match": {
                    "trackpoints": {"$ne": []} 
                }
            },
            {
                "$unwind": "$trackpoints"
            },
            {
                "$match": {
                    "trackpoints.altitude": {"$ne": -777} 
                }
            },
            {
                "$group": {
                    "_id": {"user_id": "$user_id", "activity_id": "$_id"},
                    "trackpoints": {"$push": "$trackpoints"}
                }
            }
        ]

        valid_activities = list(self.db.Activity.aggregate(query))
        print(f"Number of valid activities returned after query: {len(valid_activities)}")

        # Ensure valid activities to process
        if not valid_activities:
            print("No activities found with valid altitude data.")
            return []

        # Debug: Print structure of first valid activity to confirm correct indexing
        print("Sample structure of first valid activity:", valid_activities[0])

        # Dictionary to store total altitude gain per user
        alt_gain_per_user = {}

        # Iterate over each valid activity to compute altitude gain
        for idx, activity in enumerate(valid_activities):
            # Confirm if '_id' and 'trackpoints' are correctly accessible
            if '_id' not in activity or 'trackpoints' not in activity:
                print(f"Skipping invalid structure in activity {idx}:", activity)
                continue

            user_id = activity["_id"].get("user_id")
            trackpoints = activity["trackpoints"]

            # Ensure trackpoints list is non-empty and correctly structured
            if not trackpoints or not isinstance(trackpoints, list):
                print(f"Skipping activity {idx} due to empty or invalid trackpoints:", trackpoints)
                continue

            # Ensure trackpoints are sorted by date
            trackpoints = sorted(trackpoints, key=lambda tp: tp["date_time"])

            # Check for at least two valid trackpoints
            if len(trackpoints) < 2:
                print(f"Not enough trackpoints for altitude gain calculation in activity {idx}.")
                continue

            activity_total_gain = 0
            for i in range(1, len(trackpoints)):
                curr_alt = trackpoints[i].get("altitude")
                prev_alt = trackpoints[i - 1].get("altitude")

                # Calculate positive altitude gain
                if curr_alt and prev_alt and curr_alt > prev_alt:
                    gain = (curr_alt - prev_alt) * 0.3048
                    activity_total_gain += gain
                    print(f"Altitude gain between points {i-1} and {i}: {gain:.2f} meters")

            # Accumulate total gain for each user
            if user_id in alt_gain_per_user:
                alt_gain_per_user[user_id] += activity_total_gain
            else:
                alt_gain_per_user[user_id] = activity_total_gain

            # Debug: Show calculated gain for each activity
            print(f"Total altitude gain for Activity {activity['_id']['activity_id']}: {activity_total_gain:.2f} meters")

        # Display altitude gain per user
        print("\nAltitude gain per user:", alt_gain_per_user)

        # Sort and get top 20 users by altitude gain
        top_20_users = sorted(alt_gain_per_user.items(), key=lambda x: x[1], reverse=True)[:20]

        # Print top 20 users
        print("Top 20 users with altitude gain:")
        for user_id, gain in top_20_users:
            print(f"User ID: {user_id}, Total Gain: {gain:.2f} meters")

        # Format the results
        table_data = [(user_id, f"{total_gain:.2f} meters") for user_id, total_gain in top_20_users]
        table = tabulate(table_data, headers=["User ID", "Total Altitude Gain"], tablefmt="pretty")

        print("\nFormatted Top 20 Users with the Most Altitude Gain:")
        print(table)

        return top_20_users

        


    def query9(self):
    # Find all users who have invalid activities
    # An invalid activity is defined as an activity with consecutive trackpoints where the time deviates >= 5 minutes

        # Ensure necessary indexes exist
        self.db.Trackpoint.create_index("activity_id")
        self.db.Activity.create_index("user_id")
        
        # Retrieve users who have labels
        users_with_labels = self.db.User.distinct("_id", {"has_labels": True})

        query = [
            {
                "$match": {
                    "user_id": {"$in": users_with_labels}  # Only users with labels
                }
            },
            {
                "$lookup": {
                    "from": "Trackpoint",  # Join with Trackpoint collection
                    "localField": "_id",  # Match Activity _id with Trackpoint activity_id
                    "foreignField": "activity_id",
                    "as": "trackpoints"
                }
            },
            {
                "$match": {
                    "trackpoints.1": {"$exists": True}  # Only activities with at least 2 trackpoints
                }
            },
            {
                "$sort": {
                    "trackpoints.date_time": 1  # Sort trackpoints by date_time
                }
            },
            {
                "$project": {
                    "user_id": 1,
                    "trackpoints.date_time": 1  # Only project the user ID and trackpoint times
                }
            }
        ]

        # Get activities with their trackpoints
        activities = list(self.db.Activity.aggregate(query))
        
        # Dictionary to store invalid activity count per user
        invalid_activities_per_user = {}

        # Process each activity trackpoints to check for invalid time deviations
        for activity in activities:
            user_id = activity['user_id']
            trackpoints = activity['trackpoints']
            
            # Convert trackpoints into datetime objects
            trackpoint_times = [tp['date_time'] for tp in trackpoints]
            
            # Check if there is a time deviation of >= 5 minutes between consecutive trackpoints
            is_invalid = any(
                (trackpoint_times[i] - trackpoint_times[i - 1]).total_seconds() >= 300
                for i in range(1, len(trackpoint_times))
            )

            # If the activity is invalid, increment the count for the user
            if is_invalid:
                if user_id in invalid_activities_per_user:
                    invalid_activities_per_user[user_id] += 1
                else:
                    invalid_activities_per_user[user_id] = 1

        # Sort users by user_id for output
        sorted_invalid_activities = sorted(invalid_activities_per_user.items())

        print("Users with invalid activities (time deviation >= 5 minutes):")
        for user_id, invalid_count in sorted_invalid_activities:
            pprint({
                "User ID": user_id,
                "Invalid Activities": invalid_count
            })

        return invalid_activities_per_user


    def query10(self):
        # Users who have tracked an activity in the Forbidden City of Beijing
        # Forbidden City of Beijing coordinates: lat 39.916, lon 116.397
        # Find trackpoints tracked in the forbidden city
        
        # Debug: Count total trackpoints
        total_trackpoints = self.db.Trackpoint.count_documents({})
        print(f"Total Trackpoints: {total_trackpoints}")

        # Debug: Count trackpoints in the specified coordinate range
        trackpoints_in_range = self.db.Trackpoint.count_documents({
            "lat": {"$gte": 39.915, "$lte": 39.917},
            "lon": {"$gte": 116.396, "$lte": 116.398}
        })
        print(f"Trackpoints in Forbidden City range: {trackpoints_in_range}")

        query = [
            {
                "$match": {
                    "lat": {"$gte": 39.915, "$lte": 39.917},  # Latitude range
                    "lon": {"$gte": 116.396, "$lte": 116.398}  # Longitude range
                }
            },
            {
                "$lookup": {
                    "from": "Activity",  # Join with Activity collection
                    "localField": "activity_id",  # Match Trackpoint activity_id with Activity id
                    "foreignField": "_id",
                    "as": "activity_info"  # Rename the joined Activity info
                }
            },
            {
                "$unwind": "$activity_info"  # Flatten the joined data
            },
            {
                "$group": {
                    "_id": "$activity_info.user_id"  # Group by user_id
                }
            }
        ]

        # Execute the aggregation query
        users_forbidden_city = list(self.db.Trackpoint.aggregate(query))

        # Extract user IDs from the result
        user_ids = [user["_id"] for user in users_forbidden_city]

        # Print the results using prettyprint
        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        if user_ids:
            for user_id in user_ids:
                pprint({"User ID": user_id})
        else:
            print("No users found.")

        return user_ids

    def query11(self):
        # All users who have registered transportation_mode and their most used transportation_mode
        # The answer should be in the format (user_id, most_used_transportation_mode) sorted by user_id
        # Do not count the rows where the mode is null

        query = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None}  # Exclude rows with null transportation_mode
                }
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "mode_count": {"$sum": 1}  # Count occurrences of each transportation mode for each user
                }
            },
            {
                "$sort": {
                    "_id.user_id": 1,       # Sort by user_id
                    "mode_count": -1        # Sort by mode_count in descending order within each user's modes
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_transportation_mode": {"$first": "$_id.transportation_mode"},  # Select the first (most-used mode) per user
                    "mode_count": {"$first": "$mode_count"}  # Get the count for the most-used mode
                }
            },
            {
                "$project": {
                    "user_id": "$_id",
                    "most_used_transportation_mode": 1,
                    "_id": 0  # Exclude the _id field from the final output
                }
            },
            {
                "$sort": {"user_id": 1}  # Sort the final output by user_id
            }
        ]

        # Execute the aggregation query
        transportation_modes = list(self.db.Activity.aggregate(query))

        # Format the results as a list of tuples (user_id, most_used_transportation_mode)
        result = [(mode["user_id"], mode["most_used_transportation_mode"]) for mode in transportation_modes]

        print("Users who have registered transportation_mode and their most used transportation_mode:")
        for user_id, transportation_mode in result:
            pprint({"User ID": user_id, "Most used transportation mode": transportation_mode})

        return result    
  
    
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