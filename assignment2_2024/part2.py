import math
from DbConnector import DbConnector

class GeolifeAnalysisTask:
    def __init__(self):
        # Establishing the connection to the MySQL database using DbConnector
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def count_entries(self):
        # Count users
        self.cursor.execute("SELECT COUNT(*) FROM User")
        user_count = self.cursor.fetchone()[0]
        
        # Count activities
        self.cursor.execute("SELECT COUNT(*) FROM Activity")
        activity_count = self.cursor.fetchone()[0]
        
        # Count trackpoints
        self.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
        trackpoint_count = self.cursor.fetchone()[0]
        
        print(f"Users: {user_count}, Activities: {activity_count}, TrackPoints: {trackpoint_count}")
        return user_count, activity_count, trackpoint_count

    def average_activities_per_user(self):
        self.cursor.execute("SELECT COUNT(*) / (SELECT COUNT(*) FROM User) FROM Activity")
        avg_activities = self.cursor.fetchone()[0]
        print(f"Average number of activities per user: {avg_activities}")
        return avg_activities

    def top_20_users(self):
        self.cursor.execute("""
        SELECT user_id, COUNT(*) as activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20
        """)
        result = self.cursor.fetchall()
        print("Top 20 users with the highest number of activities:")
        for row in result:
            print(f"User ID: {row[0]}, Activities: {row[1]}")
        return result

    def users_taken_taxi(self):
        self.cursor.execute("""
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'taxi'
        """)
        users = self.cursor.fetchall()
        print("Users who have taken a taxi:")
        for user in users:
            print(f"User ID: {user[0]}")
        return users

    def count_transportation_modes(self):
        self.cursor.execute("""
        SELECT transportation_mode, COUNT(*) as activity_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        """)
        modes = self.cursor.fetchall()
        print("Transportation modes and their activity counts:")
        for row in modes:
            print(f"Mode: {row[0]}, Activities: {row[1]}")
        return modes

    def year_with_most_activities(self):
        self.cursor.execute("""
        SELECT YEAR(start_date_time) as year, COUNT(*) as activity_count
        FROM Activity
        GROUP BY year
        ORDER BY activity_count DESC
        LIMIT 1
        """)
        result = self.cursor.fetchone()
        if result:
            print(f"Year with the most activities: {result[0]} ({result[1]} activities)")
        else:
            print("No activities found.")
        return result

    def year_with_most_recorded_hours(self):
        self.cursor.execute("""
        SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as total_hours
        FROM Activity
        GROUP BY year
        ORDER BY total_hours DESC
        LIMIT 1
        """)
        result = self.cursor.fetchone()
        if result:
            print(f"Year with the most recorded hours: {result[0]} ({result[1]} hours)")
        else:
            print("No activities found.")
        return result

    def haversine(self, lat1, lon1, lat2, lon2):
        # Earth radius in kilometers
        R = 6371  
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c  # Distance in kilometers

    def total_distance_walked_2008(self, user_id='112'):
        self.cursor.execute("""
        SELECT T.latitude, T.longitude
        FROM TrackPoint T
        JOIN Activity A ON T.activity_id = A.id
        WHERE A.user_id = %s AND A.transportation_mode = 'walk' AND YEAR(A.start_date_time) = 2008
        ORDER BY T.date_time
        """, (user_id,))
        trackpoints = self.cursor.fetchall()

        total_distance = 0.0
        for i in range(1, len(trackpoints)):
            lat1, lon1 = trackpoints[i - 1]
            lat2, lon2 = trackpoints[i]
            total_distance += self.haversine(lat1, lon1, lat2, lon2)  # Use self.haversine here
        
        print(f"Total distance walked by user {user_id} in 2008: {total_distance:.2f} km")
        return total_distance
    
    def top_20_altitude_gains(self):
        self.cursor.execute("""
        SELECT A.user_id, SUM(GREATEST(0, T1.altitude - T2.altitude)) AS total_gain
        FROM TrackPoint T1
        JOIN TrackPoint T2 ON T1.id = T2.id + 1
        JOIN Activity A ON T1.activity_id = A.id  -- Join with Activity to get the user_id
        WHERE T1.altitude > -777  -- Exclude invalid altitude
        AND T2.altitude > -777  -- Exclude invalid altitude
        GROUP BY A.user_id
        ORDER BY total_gain DESC
        LIMIT 20;
        """)
        
        result = self.cursor.fetchall()
        print("Top 20 users with the highest altitude gain:")
        for row in result:
            print(f"User ID: {row[0]}, Total Gain: {row[1]} meters")
        return result

    def find_invalid_activities(self):
        self.cursor.execute("""
        SELECT A.user_id, COUNT(A.id) as invalid_activities
        FROM Activity A
        JOIN TrackPoint T1 ON A.id = T1.activity_id
        JOIN TrackPoint T2 ON T1.id = T2.id + 1
        WHERE TIMESTAMPDIFF(MINUTE, T1.date_time, T2.date_time) > 5
        GROUP BY A.user_id;
        """)
        
        result = self.cursor.fetchall()
        print("Users with invalid activities and number of invalid activities:")
        for row in result:
            print(f"User ID: {row[0]}, Invalid Activities: {row[1]}")
        return result
    
    def find_users_in_forbidden_city(self):
        self.cursor.execute("""
       SELECT DISTINCT A.user_id
        FROM TrackPoint T
        JOIN Activity A ON T.activity_id = A.id
        WHERE ABS(T.latitude - 39.916) < 0.005 AND ABS(T.longitude - 116.397) < 0.005;
        """)
        
        result = self.cursor.fetchall()
        print("Users who have tracked an activity in the Forbidden City:")
        for row in result:
            print(f"User ID: {row[0]}")
        return result

    def find_most_used_transport_mode(self):
        self.cursor.execute("""
        SELECT user_id, transportation_mode, COUNT(*) as mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY user_id, transportation_mode
        ORDER BY user_id, mode_count DESC;
        """)
        
        result = self.cursor.fetchall()
        user_modes = {}
        
        for row in result:
            if row[0] not in user_modes:
                user_modes[row[0]] = row[1]  # Keep only the most frequent mode
        
        print("Users and their most used transportation mode:")
        for user_id, mode in user_modes.items():
            print(f"User ID: {user_id}, Most Used Mode: {mode}")
        
        return user_modes

    
    def close_connection(self):
        self.connection.close_connection()

def main():
    task = GeolifeAnalysisTask()
    
    # Call the required methods for Part 2 questions
    task.count_entries()
    task.average_activities_per_user()
    task.top_20_users()
    task.users_taken_taxi()
    task.count_transportation_modes()
    task.year_with_most_activities()
    task.year_with_most_recorded_hours()
    task.total_distance_walked_2008()
    task.top_20_altitude_gains()
    task.find_invalid_activities()
    task.find_users_in_forbidden_city()
    task.find_most_used_transport_mode()
    
    # Close connection
    task.close_connection()

if __name__ == '__main__':
    main()
