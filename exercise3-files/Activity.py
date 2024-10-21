class Activity:
    def __init__(self, id, user_id, transportation_mode, start_date_time, end_date_time):
        self.id = id
        self.user_id = user_id
        self.transportation_mode = transportation_mode
        self.start_date_time = start_date_time
        self.end_date_time = end_date_time
        self.trackpoints = []
        self.durationlabel = 0
    
    def __str__(self):
        return f"{self.id}: {self.user_id}: {self.transportation_mode}: {self.start_date_time}: {self.end_date_time}"

    def get_data(self):
        return [self.id, self.user_id, self.transportation_mode, self.start_date_time, self.end_date_time]
    
