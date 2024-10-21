from DbConnector import DbConnector
from tabulate import tabulate 
from math import radians, sin, cos, sqrt, atan2

class ProgramQueries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query1(self):
    ## How many users, activities and trackpoints there are in the dataset
    ## Assuming each user, activity and  has a unique id
        query_users = """SELECT COUNT(DISTINCT id) FROM User"""
    
        query_activity = """SELECT COUNT(DISTINCT id) FROM Activity"""
        
        query_trackpoint = """SELECT COUNT(DISTINCT id) FROM Trackpoint"""
        
        self.cursor.execute(query_users)
        users = self.cursor.fetchall()

        self.cursor.execute(query_activity)
        activities = self.cursor.fetchall()

        self.cursor.execute(query_trackpoint)   
        trackpoints = self.cursor.fetchall()

        users_count = users[0][0]
        activities_count = activities[0][0]
        trackpoints_count = trackpoints[0][0]

        print("Number of users:", users_count, "Number of activities:", activities_count, "Number of trackpoints:", trackpoints_count)
        return users, activities, trackpoints

    
    def query2(self):
    # Find the average number of activities per user
        query = """
                SELECT AVG(activities_per_user) 
                FROM (SELECT COUNT(id) AS activities_per_user 
                    FROM Activity 
                    GROUP BY user_id) AS user_activity_count
                """
        
        self.cursor.execute(query)
        avg_activities = self.cursor.fetchone()[0]  # Extract the first value
        print("Average number of activities per user:", avg_activities)
        return avg_activities


    def query3(self):
    # The top 20 users with the highest number of activities
        query = """
                SELECT user_id, COUNT(id) AS activity_count 
                FROM Activity 
                GROUP BY user_id 
                ORDER BY activity_count DESC 
                LIMIT 20
                """
        
        self.cursor.execute(query)
        top_20_users = self.cursor.fetchall()

        # Print the top 20 users with the highest number of activities
        print("Top 20 users with the highest number of activities:")
        for idx, (user_id, activity_count) in enumerate(top_20_users, 1):
            print(f"{idx}. User id: {user_id}, Number of activities: {activity_count}")
        
        return top_20_users

    
    def query4(self):
    # All users who have taken a taxi
        query = """
                SELECT DISTINCT user_id 
                FROM Activity 
                WHERE transportation_mode = 'taxi'
                """
        
        self.cursor.execute(query)
        taxi_users = self.cursor.fetchall()

        # Print the users who have taken a taxi
        print("Users who have taken a taxi:")
        for idx, (user_id,) in enumerate(taxi_users, 1):
            print(f"{idx}. User id: {user_id}")
        
        return taxi_users

    
    def query5(self):
    # All types of transportation modes and count how many activities that are tagged with these transportation mode labels
    # Do not count the rows where the mode is null
        query = """
                SELECT transportation_mode, COUNT(id) AS activity_count 
                FROM Activity 
                WHERE transportation_mode IS NOT NULL 
                ANd user_id IN (SELECT id FROM User WHERE has_labels = 1)
                GROUP BY transportation_mode
                """
        # Only consider activities where users have tagged them

        self.cursor.execute(query)
        transportation_modes = self.cursor.fetchall()

        # Print the transportation modes and the count of activities tagged with these modes
        print("Transportation modes and the count of activities tagged with these modes:")
        for idx, (transportation_mode, activity_count) in enumerate(transportation_modes, 1):
            print(f"{idx}. Transportation mode: {transportation_mode}, Number of activities: {activity_count}")
        
        return transportation_modes


    def query6(self):
    # a) The year with the most activities
    # b) Is this also the year with the most recorded hours?
        query_year_most_activties = """
                                    SELECT YEAR(end_date_time) AS year, COUNT(id) AS activity_count
                                    FROM Activity
                                    GROUP BY year
                                    ORDER BY activity_count DESC
                                    LIMIT 1
                                    """
        
        query_year_most_hours = """
                                WITH Activityhours AS (
                                    SELECT YEAR(end_date_time) AS year,
                                        TIMESTAMPDIFF(SECOND, start_date_time, end_date_time) / 3600 AS activity_hours
                                    FROM Activity
                                )
                                SELECT year, SUM(activity_hours) AS total_hours
                                FROM Activityhours
                                GROUP BY year
                                ORDER BY total_hours DESC
                                LIMIT 1
                                """
        
        self.cursor.execute(query_year_most_activties)
        year_most_activities = self.cursor.fetchone()
        self.cursor.execute(query_year_most_hours)
        year_most_hours = self.cursor.fetchone()

        # Compare year most activities with year most hours
        if year_most_activities[0] == year_most_hours[0]:
            print(f"The year {year_most_activities[0]} has the most activities and the most recorded hours")
        else:
            print(f"The year {year_most_activities[0]} has the most activities, but the year {year_most_hours[0]} has the most recorded hours")
        
        return year_most_activities, year_most_hours


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
        
        # Find all activities labeled as walking for user with id=112 in 2008
        query_walking_activities = """
                                    SELECT id
                                    FROM Activity
                                    WHERE user_id = 112
                                    AND transportation_mode = 'walk'
                                    AND YEAR(start_date_time) = 2008
                                    """ 
        self.cursor.execute(query_walking_activities)
        walking_activities = self.cursor.fetchall()

        total_distance = 0

        # For each activity, get the trackpoints and calculate the total distance
        for activity in walking_activities:
            activity_id = activity[0]
            query_trackpoints = """
                                SELECT lat, lon
                                FROM Trackpoint
                                WHERE activity_id = %s
                                ORDER BY id
                                """
            self.cursor.execute(query_trackpoints, (activity_id,))
            trackpoints = self.cursor.fetchall()

            # Calculate the distance between consecutive trackpoints
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i-1]
                lat2, lon2 = trackpoints[i]
                total_distance += self.haversine(lat1, lon1, lat2, lon2)

        print(f"Total distance walked by user with id=112 in 2008: {total_distance:.2f} km")
        return total_distance
    

    def query8(self):
    # Find the top 20 users who have gained the most altitude meters
    # Output should be a table with (id, total meters gained per user)
    # Remember that some altitude meters are invalid 
    # Tip: math formula (look at task sheet)¨
    # Note: Remember that we are looking for altitude GAIN 

        # Get all trackpoints for all users
        query_trackpoints = """
                            SELECT Activity.user_id, Activity.id AS activity_id, Trackpoint.altitude
                            FROM Activity
                            INNER JOIN Trackpoint ON Activity.id = Trackpoint.activity_id
                            WHERE Trackpoint.altitude != -777
                            ORDER BY Activity.user_id, Trackpoint.activity_id, Trackpoint.id
                            """

        self.cursor.execute(query_trackpoints)
        trackpoints = self.cursor.fetchall()

        # Dictionary to store altitudes per user and activity
        alt_per_user = {}

        # Store all altitudes per user and activity
        for user_id, activity_id, current_altitude in trackpoints:
            alt_per_user.setdefault(user_id, {}).setdefault(activity_id, []).append(current_altitude)

        # Dictionary to store total altitude gain per user
        alt_gain_per_user = {}

        # Calculate altitude gain for each user and activity
        for user_id, activities in alt_per_user.items():
            user_total_gain = 0  # Total altitude gain for this user

            for activity_id, altitudes in activities.items():
                if len(altitudes) < 2:  # At least 2 trackpoints needed to calculate alt gain
                    continue

                # Using formula from exercise sheet to calculate altitude gain
                activity_total_gain = 0
                for i in range(1, len(altitudes)):  # Start from the second trackpoint
                    current_altitude = altitudes[i]
                    previous_altitude = altitudes[i - 1]

                    if current_altitude > previous_altitude:
                        activity_total_gain += (current_altitude - previous_altitude)

                # Add current activity alt gain to user total
                user_total_gain += activity_total_gain

            # Store total altitude gain for the user
            alt_gain_per_user[user_id] = user_total_gain

        # Sort users by total altitude gain in descending order and get the top 20
        top_20_users = sorted(alt_gain_per_user.items(), key=lambda x: x[1], reverse=True)[:20]

        # Format the results as a table using tabulate and print it
        table_data = [(user_id, f"{total_gain:.2f} meters") for user_id, total_gain in top_20_users]
        table = tabulate(table_data, headers=["User ID", "Total Altitude Gain"], tablefmt="pretty")

        print("Top 20 users with the most altitude gain:")
        print(table)

        return top_20_users
        
        # Spørsmål: Holder d å printe et table eller skal man skrive et nytt table til selve databasen?


    def query9(self):
    # All users who have invalid activities, and the number of invalid activities per user
    # An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes

        query_trackpoints = """
                        SELECT Activity.user_id, Activity.id AS activity_id, Trackpoint.date_time AS trackpoint_time
                        FROM Activity
                        INNER JOIN Trackpoint ON Activity.id = Trackpoint.activity_id
                        ORDER BY Activity.user_id, Activity.id, Trackpoint.date_time
                        """
    
        self.cursor.execute(query_trackpoints)
        trackpoints = self.cursor.fetchall()

        # Dictionary to store invalid activity count per user
        invalid_activities_per_user = {}

        # Dictionary to group trackpoints by user and activity
        trackpoints_per_activity = {}

        # Organize trackpoints by activity and user
        for user_id, activity_id, trackpoint_time in trackpoints:
            if (user_id, activity_id) not in trackpoints_per_activity:
                trackpoints_per_activity[(user_id, activity_id)] = []
            trackpoints_per_activity[(user_id, activity_id)].append(trackpoint_time)

        # Iterate over each activity and check for invalid activities
        for (user_id, activity_id), times in trackpoints_per_activity.items():
            # Check if there are enough trackpoints
            if len(times) < 2:
                continue

            # Determine if an activity is invalid (300 seconds = 5 minutes)
            is_invalid = any(
                (times[i] - times[i - 1]).total_seconds() >= 300 for i in range(1, len(times))
            )

            # If the activity is invalid, update the count for the user
            if is_invalid:
                if user_id in invalid_activities_per_user:
                    invalid_activities_per_user[user_id] += 1
                else:
                    invalid_activities_per_user[user_id] = 1

        print("Users with invalid activities (time deviation >= 5 minutes):")
        for user_id, invalid_count in invalid_activities_per_user.items():
            print(f"User ID: {user_id}, Invalid activities: {invalid_count}")

        return invalid_activities_per_user


    def query10(self):
    # Users who have tracked an activity in the Forbidden City of Beijing
    # Forbidden City of Beijing coordinates: lat 39.916, lon 116.397
    # Find trackpoints tracked in the forbidden city

        query_trackpoints_forbidden_city = """
                                            SELECT DISTINCT Activity.user_id
                                            FROM Trackpoint
                                            INNER JOIN Activity ON Trackpoint.activity_id = Activity.id
                                            WHERE Trackpoint.lat BETWEEN 39.915 AND 39.917
                                            AND Trackpoint.lon BETWEEN 116.396 AND 116.398
                                            """
        self.cursor.execute(query_trackpoints_forbidden_city)
        users_forbidden_city = self.cursor.fetchall()

        # Get user IDs from the query result
        user_ids = [user_id for (user_id,) in users_forbidden_city]

        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        for user_id in user_ids:
            print(f"User ID: {user_id}")

        return user_ids

    
    def query11(self):
    # All users who have registered transportation_mode and their most used transportation_mode
    # The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id
    # Some users may have the same number of activities tagged with e.g. walk and car. 
    # In this case it is up to you to decide which transportation mode to include in your answer (choose one)
    # Do not count the rows where the mode is null

        query_transportation_modes = """
                                        SELECT Activity.user_id, Activity.transportation_mode, COUNT(Activity.id) AS mode_count
                                        FROM Activity
                                        WHERE Activity.transportation_mode IS NOT NULL
                                        GROUP BY Activity.user_id, Activity.transportation_mode
                                    """
        self.cursor.execute(query_transportation_modes)
        transportation_modes = self.cursor.fetchall()

        # Dictionary to store the most used transportation mode for each user
        user_modes = {}

        for user_id, transportation_mode, mode_count in transportation_modes:
            # If user is not in the dictionary or current mode has a higher count -> update the entry
            if user_id not in user_modes or mode_count > user_modes[user_id][1]:
                user_modes[user_id] = (transportation_mode, mode_count)

        # Format the results as a list of tuples (user_id, most_used_transportation_mode)
        result = [(user_id, transportation_mode) for user_id, (transportation_mode, _) in sorted(user_modes.items())]

        print("Users who have registered transportation_mode and their most used transportation_mode:")
        for user_id, transportation_mode in result:
            print(f"User ID: {user_id}, Most used transportation mode: {transportation_mode}")

        return result  
    
    def close_connection(self):
        self.connection.close_connection()
            
def main():
    program = None
    try:
        program = ProgramQueries()
        program.query1()
        program.query2()
        program.query3()
        program.query4()
        program.query5()
        program.query6()
        program.query7()
        program.query8()
        program.query9()
        program.query10()
        program.query11()
    except Exception as e:
        print("ERROR: Failed to insert data:", e)
    finally:
        if program:
            program.close_connection()

if __name__ == "__main__":
    main()