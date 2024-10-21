# Python file with functions to extract data from the dataset, set labels to users and to activities.

import os
from Activity import Activity
from Trackpoint import Trackpoint
from User import User
from datetime import datetime
import math

######### CHANGE THIS PATH TO YOUR OWN PATH #########
path = "/Users/andreasbuervase/Desktop/4.klasse Indøk/Store, distribuerte datamengder/Group project/tdt4225/assignment2_2024/dataset/dataset"
####################################################

# Function which goes through all the .tlp files in /assignment2_2024/dataset/dataset/Data, extract the data and create User, Activity and Trackpoint objects
def extract_data():
    # Get all directories in Data
    directories = os.listdir(path + "/Data")
    # Create a list to store all users
    users = []
    # Filter out non-numeric files (like .DS_Store) and sort the numeric filenames
    numeric_directories = [directory for directory in directories if directory.isdigit()]
    sorted_directories = sorted(numeric_directories, key=lambda x: int(x))  # Sort numerically
    ID_datapoints = 0
    ID_Activity = 0
    # Go through each directory
    for directory in sorted_directories:
        # Create a new user with the id of the directory
        user = User(directory, False)
        # Get all files in the directory using a for loop
        files = os.listdir(path + f"/Data/{directory}/Trajectory")
        # Filter out .DS_Store files
        files = [file for file in files if not file.startswith(".")]
        # Create a list to store all activities
        activities = []
        # Go through each file
        for file in files:
            # Check if the file is a .plt file
            if file.endswith(".plt"):
                # Open the file and read the content
                with open(path + f"/Data/{directory}/Trajectory/{file}", "r") as f:
                    lines = f.read().splitlines()
                    # Checks if the file is longer 2507 linees and yes, skips the file
                    if len(lines) <= 2506:
                        # Get the start and end date time from the first and last trackpoints
                        start_date_time = datetime.strptime(lines[7].split(',')[-2] + " " + lines[7].split(',')[-1], "%Y-%m-%d %H:%M:%S")
                        end_date_time = datetime.strptime(lines[-1].split(',')[-2] + " " + lines[-1].split(',')[-1], "%Y-%m-%d %H:%M:%S")
                        # Create a new activity with the id of the directory
                        activity = Activity(ID_Activity, directory, None , start_date_time, end_date_time)
                        ID_Activity += 1
                        # Create a list to store all trackpoints
                        trackpoints = []
                        # Go through each line in the file, dropping the first part of the file
                        for line in lines[6:]:
                            # Split the line by comma
                            data = line.split(",")
                            # Create a new trackpoint
                            date_time = datetime.strptime(data[5] + " " + data[6], "%Y-%m-%d %H:%M:%S")
                            trackpoint = Trackpoint(ID_datapoints, ID_Activity, data[0], data[1], data[3], date_time)
                            ID_datapoints += 1
                            # Add the trackpoint to the list
                            trackpoints.append(trackpoint)
                        # Set the trackpoints for the activity
                        activity.trackpoints = trackpoints
                        # Add the activity to the list
                        activities.append(activity)
        
        # Set the activities for the user
        user.activities = activities
        # Add the user to the list
        users.append(user)
    # Return the list of users
    return users

# Function to set User.has_labels to True if the user has labels, going through the labeled_ids.txt file 
def set_has_labels(users):
    # Open file "labeled_ids.txt" and read the content
    with open(path + "/labeled_ids.txt", "r") as file:
        labeled_ids = file.read().splitlines()
    # Go through each user
    for user in users:
        # Check if the user id is in labeled_ids
        if user.id in labeled_ids:
            # Set has_labels to True
            user.has_labels = True
    # Return the users
    return users

# Function which goes through all the "labels.txt" files in /assignment2_2024/dataset/dataset/Data and add the labels to the activities
def add_labels(users):
    directories = os.listdir(path + "/Data")
    
    # Filter out non-numeric files (like .DS_Store) and sort the numeric filenames
    numeric_directories = [directory for directory in directories if directory.isdigit()]
    sorted_directories = sorted(numeric_directories, key=lambda x: int(x))  # Sort numerically
    for directory in sorted_directories:
        files = os.listdir(path + f"/Data/{directory}")
        # Filter out .DS_Store files
        files = [file for file in files if not file.startswith(".")]
        # Go through each file
        for file in files:
            # Check if the file is a labels.txt file
            if file == "labels.txt":
                # Open the file and read the content
                with open(path + f"/Data/{directory}/{file}", "r") as f:
                    lines = f.read().splitlines()
                    for line in lines[1:]:  # Skip header:
                        data = line.split("\t")
                        # Get data from labels.txt
                        start_time = datetime.strptime(data[0], "%Y/%m/%d %H:%M:%S")
                        end_time = datetime.strptime(data[1], "%Y/%m/%d %H:%M:%S")
                        transportation_mode = data[2]
                        for user in users:
                            if user.id == directory:
                                for activity in user.activities:
                                    # Scenario where the label is "inside" the activity
                                    if activity.start_date_time <= start_time and end_time <= activity.end_date_time:
                                        if (activity.durationlabel < (end_time - start_time).total_seconds()):
                                            activity.transportation_mode = transportation_mode
                                    # Scenario where the activity is "inside" the label
                                    elif start_time <= activity.start_date_time and  activity.end_date_time <= end_time:
                                        if (activity.durationlabel < (end_time - start_time).total_seconds()):
                                            activity.transportation_mode = transportation_mode
                    
                            
    return users


###### TESTING THE QUERY FUNCTIONS #####
# These tests are mainly written by AI-tools #

# Query 1
# Make a function which counts the number of users, activities and trackpoints
def count(users):
    num_users = len(users)
    num_activities = 0
    num_trackpoints = 0
    for user in users:
        num_activities += len(user.activities)
        for activity in user.activities:
            num_trackpoints += len(activity.trackpoints)
    return num_users, num_activities, num_trackpoints

# Query 2
# Function which finds the average number of activities per user
def average_activities(users):
    num_users = len(users)
    num_activities = 0
    for user in users:
        num_activities += len(user.activities)
    return num_activities / num_users

# Query 3
# Function which finds top 20 users with most activities
def top_20(users):
    users.sort(key=lambda x: len(x.activities), reverse=True)
    return users[:20]

# Query 4
# Function which finds all users who have taken a taxi
def taxi_users(users):
    taxi_users = []
    for user in users:
        for activity in user.activities:
            if activity.transportation_mode == "taxi":
                taxi_users.append(user)
                break
    return taxi_users


# Query 5
# Function which counts the number of activities for each transportation mode
def count_transportation_modes(users):
    transportation_modes = {}
    for user in users:
        for activity in user.activities:
            if activity.transportation_mode in transportation_modes:
                transportation_modes[activity.transportation_mode] += 1
            else:
                transportation_modes[activity.transportation_mode] = 1
    return transportation_modes

# Query 6
# Find the year with the most activities and year with most recorded hours of activities
def most_activities_and_hours(users):
    years = {}
    for user in users:
        for activity in user.activities:
            year = activity.start_date_time.year
            if year in years:
                years[year][0] += 1
                years[year][1] += (activity.end_date_time - activity.start_date_time).total_seconds()
            else:
                years[year] = [1, (activity.end_date_time - activity.start_date_time).total_seconds()]
    most_activities = max(years, key=lambda x: years[x][0])
    most_hours = max(years, key=lambda x: years[x][1])
    return most_activities, most_hours

# Query 7
# Find the total distance with transportation mode "walk" for user "112" in 2008, using haversine formula

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Radius of the Earth in meters
    phi1 = lat1 * (3.14159265359 / 180)
    phi2 = lat2 * (3.14159265359 / 180)
    delta_phi = (lat2 - lat1) * (3.14159265359 / 180)
    delta_lambda = (lon2 - lon1) * (3.14159265359 / 180)
    a = (pow(math.sin(delta_phi/2), 2) + math.cos(phi1) * math.cos(phi2) * pow(math.sin(delta_lambda/2), 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def total_distance_walk(users):
    total_distance = 0
    for user in users:
        if user.id == "112":
            for activity in user.activities:
                if activity.transportation_mode == "walk" and activity.start_date_time.year == 2008:
                    for i in range(len(activity.trackpoints) - 1):
                        lat1 = float(activity.trackpoints[i].lat)
                        lon1 = float(activity.trackpoints[i].lon)
                        lat2 = float(activity.trackpoints[i+1].lat)
                        lon2 = float(activity.trackpoints[i+1].lon)
                        total_distance += haversine(lat1, lon1, lat2, lon2)
    return total_distance




# Query 9
# Find all users who have invalid activities, and the number of invalid activities per user
# ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
def invalid_activities(users):
    invalid_activities = {}
    for user in users:
        for activity in user.activities:
            for i in range(len(activity.trackpoints) - 1):
                if (activity.trackpoints[i+1].date_time - activity.trackpoints[i].date_time).total_seconds() > 300:
                    if user.id in invalid_activities:
                        invalid_activities[user.id] += 1
                    else:
                        invalid_activities[user.id] = 1
                    break
    return invalid_activities

# Query 10
# Find the users who have tracked an activity in the Forbidden City of Beijing.
# In this question you can consider the Forbidden City to have
# coordinates that correspond to: lat 39.916 +- 1, lon 116.397 +-1.
def forbidden_city(users):
    forbidden_city_users = set()  # Use a set to avoid duplicate users
    for user in users:
        for activity in user.activities:
            for trackpoint in activity.trackpoints:
                lat, lon = float(trackpoint.lat), float(trackpoint.lon)
                if 39.916 - 0.001 <= lat <= 39.916 + 0.001 and 116.397 - 0.001 <= lon <= 116.397 + 0.001:
                    forbidden_city_users.add(user)  # Add user id to the set
                    break  # Stop checking further trackpoints for this activity
            if user.id in forbidden_city_users:
                break  # Stop checking further activities once user is added to the set
    return list(forbidden_city_users)  # Return list of users

# Query 11
# Find all users who have registered transportation_mode and their most used transportation_mode.
# The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
# When a user has the same amount of the most used transportation_mode, the first one should be chosen.
# Do not count the rows where the mode is null

def most_used_transportation_mode(users):
    result = []
    for user in sorted(users, key=lambda x: x.id):  # Ensure sorting by user_id
        transportation_modes = {}
        for activity in user.activities:
            if activity.transportation_mode is not None:
                if activity.transportation_mode in transportation_modes:
                    transportation_modes[activity.transportation_mode] += 1
                else:
                    transportation_modes[activity.transportation_mode] = 1
        
        if transportation_modes:
            # Sort by count (descending) and by insertion order (in case of a tie)
            most_used_mode = max(transportation_modes, key=lambda x: (transportation_modes[x], -list(transportation_modes.keys()).index(x)))
            result.append((user.id, most_used_mode))
    
    return result


if __name__ == "__main__":
    # Extract the data
    users = extract_data()
    # Set has_labels to True for the users with labels
    users = set_has_labels(users)
    # Add the labels to the activities
    users = add_labels(users)
    # Printing the users, activities and trackpoints
    num_users, num_activities, num_trackpoints = count(users)
    print(f"Number of users: {num_users}")
    print(f"Number of activities: {num_activities}")
    print(f"Number of trackpoints: {num_trackpoints}")
    print(f"Average number of activities per user: {average_activities(users)}")
    print(f"Users who have taken a taxi: {len(taxi_users(users))}")
    print("Top 20 users with most activities:")
    for user in top_20(users):
        print(f"User {user.id}: {len(user.activities)} activities")
    print("Number of activities for each transportation mode:")
    transportation_modes = count_transportation_modes(users)
    for key, value in transportation_modes.items():
        print(f"{key}: {value} activities")
    most_activities, most_hours = most_activities_and_hours(users)
    print(f"Year with most activities: {most_activities}")
    print(f"Year with most recorded hours of activities: {most_hours}")
    print(f"Total hours with transportation mode 'walk' for user '112' in 2008: {total_distance_walk(users)}")
    for key, value in invalid_activities(users).items():
        print(f"User {key}: {value} invalid activities")
    print("Users who have tracked an activity in the Forbidden City of Beijing:")
    for user in forbidden_city(users):
        print(f"User {user.id}")
    print("Users who have registered transportation_mode and their most used transportation_mode:")
    for user in most_used_transportation_mode(users):
        print(f"User {user[0]}: {user[1]}")

    





