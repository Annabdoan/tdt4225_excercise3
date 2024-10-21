
class Trackpoint:
    def __init__(self, ID, activity_id, lat, lon, altitude, date_time):
        self.ID = ID
        self.activity_id = activity_id
        self.lat = lat
        self.lon = lon
        self.altitude = altitude
        self.date_time = date_time
    
    def __str__(self):
        return f"{self.ID}: {self.activity_id}: {self.lat}: {self.lon}: {self.altitude}: {self.date_time}"
    
    def get_data(self):
        return [self.ID, self.activity_id, self.lat, self.lon, self.altitude, self.date_time]

    