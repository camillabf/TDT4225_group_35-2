import math
from DbConnector import DbConnector
from tabulate import tabulate

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
        query = "SELECT COUNT(*) / (SELECT COUNT(*) FROM User) FROM Activity"
        self.cursor.execute(query)
        avg_activities = self.cursor.fetchone()[0]
        print(f"Average number of activities per user: {avg_activities}")
        return avg_activities

    def top_20_users(self):
        query = """
        SELECT user_id, COUNT(*) as activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("Top 20 users with the highest number of activities:")
        print(tabulate(result, headers=['User ID', 'Activity Count'], tablefmt='grid'))

    def users_taken_taxi(self):
        query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'taxi'
        """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        print("Users who have taken a taxi:")
        print(tabulate(users, headers=['User ID'], tablefmt='grid'))
    
    def count_transportation_modes(self):
        query = """
        SELECT transportation_mode, COUNT(*) as activity_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY activity_count DESC
        """
        self.cursor.execute(query)
        modes = self.cursor.fetchall()
        print("Transportation modes and their activity counts:")
        print(tabulate(modes, headers=['Mode', 'Activity Count'], tablefmt='grid'))

    def year_with_most_activities(self):
        query = """
        SELECT YEAR(start_date_time) as year, COUNT(*) as activity_count
        FROM Activity
        GROUP BY year
        ORDER BY activity_count DESC
        LIMIT 1
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print(f"Year with the most activities: {result[0]} ({result[1]} activities)")
        return result

    def year_with_most_recorded_hours(self):
        query = """
        SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as total_hours
        FROM Activity
        GROUP BY year
        ORDER BY total_hours DESC
        LIMIT 1
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print(f"Year with the most recorded hours: {result[0]} ({result[1]} hours)")
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
        query = """
        SELECT T.latitude, T.longitude
        FROM TrackPoint T
        JOIN Activity A ON T.activity_id = A.id
        WHERE A.user_id = %s AND A.transportation_mode = 'walk' AND YEAR(A.start_date_time) = 2008
        ORDER BY T.date_time
        """
        self.cursor.execute(query, (user_id,))
        trackpoints = self.cursor.fetchall()

        total_distance = 0.0
        for i in range(1, len(trackpoints)):
            lat1, lon1 = trackpoints[i - 1]
            lat2, lon2 = trackpoints[i]
            total_distance += self.haversine(lat1, lon1, lat2, lon2)
        
        print(f"Total distance walked by user {user_id} in 2008: {total_distance:.2f} km")
        return total_distance

    def top_20_altitude_gains(self):
        query = """
    
            SELECT user_id, SUM(total_gain) AS total_altitude_gain
            FROM (
                SELECT A.user_id,
                    GREATEST(0, T1.altitude - LAG(T1.altitude, 1) 
                    OVER (PARTITION BY T1.activity_id ORDER BY T1.id)) AS total_gain
                FROM TrackPoint T1
                JOIN Activity A ON T1.activity_id = A.id  -- Join with Activity to get the user_id
                WHERE T1.altitude > -777  -- Exclude invalid altitude
            ) AS altitude_diffs
            GROUP BY user_id
            ORDER BY total_altitude_gain DESC
            LIMIT 20;
        
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("Top 20 users with the highest altitude gain:")
        print(tabulate(result, headers=['User ID', 'Total Gain (meters)'], tablefmt='grid'))

    def find_invalid_activities(self):
        query = """
        SELECT A.user_id, COUNT(A.id) as invalid_activities
        FROM Activity A
        JOIN TrackPoint T1 ON A.id = T1.activity_id
        JOIN TrackPoint T2 ON T1.id = T2.id + 1
        WHERE TIMESTAMPDIFF(MINUTE, T1.date_time, T2.date_time) > 5
        GROUP BY A.user_id;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("Users with invalid activities:")
        print(tabulate(result, headers=['User ID', 'Invalid Activities'], tablefmt='grid'))

    def find_users_in_forbidden_city(self):
        query = """
        SELECT DISTINCT A.user_id
        FROM TrackPoint T
        JOIN Activity A ON T.activity_id = A.id
        WHERE ABS(T.latitude - 39.916) < 0.005 AND ABS(T.longitude - 116.397) < 0.005;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("Users who have tracked an activity in the Forbidden City:")
        print(tabulate(result, headers=['User ID'], tablefmt='grid'))

    def find_most_used_transport_mode(self):
        query = """
        SELECT user_id, transportation_mode, COUNT(*) as mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY user_id, transportation_mode
        ORDER BY user_id, mode_count DESC;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        user_modes = {}
        
        for row in result:
            if row[0] not in user_modes:
                user_modes[row[0]] = row[1]  # Keep only the most frequent mode
        
        formatted_result = [(user, mode) for user, mode in user_modes.items()]
        print("Users and their most used transportation mode:")
        print(tabulate(formatted_result, headers=['User ID', 'Most Used Mode'], tablefmt='grid'))
        
    def close_connection(self):
        self.connection.close_connection()

def main():
    task = GeolifeAnalysisTask()
    # Run the required methods for Part 2 questions
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
