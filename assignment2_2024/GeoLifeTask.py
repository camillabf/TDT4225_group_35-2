from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os

class GeolifeDataProcessor:

    def __init__(self):
        self.db_connector = DbConnector()
        self.connection = self.db_connector.db_connection
        self.cursor = self.db_connector.cursor

    def create_table(self, table_name, table_definition):
        self.cursor.execute(table_definition % table_name)
        self.connection.commit()

    def retrieve_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        data = self.cursor.fetchall()
        print(f"Raw data from table {table_name}:")
        print(data)
        print(f"Formatted data from table {table_name}:")
        print(tabulate(data, headers=self.cursor.column_names))
        return data

    def remove_table(self, table_name):
        print(f"Removing table {table_name}...")
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.connection.commit()

    def list_tables(self):
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        self.connection.commit()
        print(tabulate(tables, headers=self.cursor.column_names))

    def display_table(self, table_name):
        query = "DESCRIBE %s"
        self.cursor.execute(query % table_name)
        structure = self.cursor.fetchall()
        print(tabulate(structure))

    def insert_user_table(self):
        insert_user_query = "INSERT INTO User (id, has_label) VALUES (%s, %s)"
        check_user_query = "SELECT COUNT(*) FROM User WHERE id = %s"
        update_user_label_query = "UPDATE User SET has_label = true WHERE id = %s"
        
        for user_id in range(182):
            formatted_user_id = f"{user_id:03d}"
            self.cursor.execute(check_user_query, (formatted_user_id,))
            if self.cursor.fetchone()[0] == 0:
                self.cursor.execute(insert_user_query, (formatted_user_id, False))
                self.connection.commit()

        with open(r"dataset/labeled_ids.txt", 'r') as file:
            labeled_users = [line.strip() for line in file]
            self.cursor.executemany(update_user_label_query, [(user_id,) for user_id in labeled_users])
            self.connection.commit()

    def insert_activity(self, user_id, transportation_mode, start_time, end_time):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(query, (user_id, transportation_mode, start_time, end_time))
        self.connection.commit()
        return self.cursor.lastrowid
    
    def batch_insert_trackpoints(self, trackpoints, batch_size=1000):
        query = """
        INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        for i in range(0, len(trackpoints), batch_size):
            batch = trackpoints[i:i+batch_size]
            self.cursor.executemany(query, batch)
        self.connection.commit()

    def parse_labels(self, labels_file_path):
        labels = []
        with open(labels_file_path, 'r') as file:
            next(file)
            for line in file:
                start_time, end_time, mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')  
                end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')  
                labels.append((start_time, end_time, mode))
        return labels
    
    def process_plt_file(self, plt_file_path, user_id, labels):
        try:
            with open(plt_file_path, 'r') as file:
                lines = file.readlines()[6:]  # Skip the header (first 6 lines)

                # Check if the activity exceeds 2500 trackpoints
                if len(lines) > 2500:
                    return  # Skip this activity and do not insert anything

                # Process start and end time
                start_line = lines[0].strip().split(',')
                end_line = lines[-1].strip().split(',')

                try:
                    start_date_time = datetime.strptime(f"{start_line[5].strip()} {start_line[6].strip()}", '%Y-%m-%d %H:%M:%S')
                    end_date_time = datetime.strptime(f"{end_line[5].strip()} {end_line[6].strip()}", '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    print(f"Error parsing date and time for file {plt_file_path}: {e}")
                    return  # Skip this file if parsing fails

                # Match label based on start and end time
                label = next((lbl[2] for lbl in labels if lbl[0] == start_date_time and lbl[1] == end_date_time), None)
                if not label:
                    return  # Skip if no matching label is found

                # Insert the activity
                activity_id = self.insert_activity(user_id, label, start_date_time, end_date_time)

                # Prepare trackpoints for insertion
                trackpoints = []
                for line in lines:
                    lat, lon, _, altitude, _, date, time = line.strip().split(',')
                    try:
                        date_time = datetime.strptime(f"{date.strip()} {time.strip()}", '%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        print(f"Skipping trackpoint with invalid date/time format: {e}")
                        continue  # Skip trackpoint if the date-time format is invalid
                    
                    trackpoints.append((activity_id, float(lat), float(lon), int(float(altitude)), date_time))

                # Insert trackpoints in batches
                self.batch_insert_trackpoints(trackpoints)

        except Exception as e:
            print(f"Error processing .plt file {plt_file_path}: {e}")

    def process_geolife_data(self, data_directory):
        for user_folder in os.listdir(data_directory):
            user_id = user_folder
            trajectory_folder = os.path.join(data_directory, user_id, 'Trajectory')
            labels_file = os.path.join(data_directory, user_id, 'labels.txt')

            labels = self.parse_labels(labels_file) if os.path.exists(labels_file) else []

            if os.path.isdir(trajectory_folder):
                for plt_file in os.listdir(trajectory_folder):
                    if plt_file.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_folder, plt_file)
                        self.process_plt_file(plt_file_path, user_id, labels)
    
    def display_top10_rows(self):
        tables = ['User', 'Activity', 'TrackPoint']
        for table in tables:
            query = f"SELECT * FROM {table} LIMIT 20;"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            print(f"Top rows from {table}:")
            print(tabulate(rows, headers=self.cursor.column_names))
            print()

# Tables
user_table = """CREATE TABLE IF NOT EXISTS %s (
                           id VARCHAR(3) NOT NULL PRIMARY KEY,
                           has_label BOOLEAN)"""

activity_table = """CREATE TABLE IF NOT EXISTS %s (
                               id INT AUTO_INCREMENT PRIMARY KEY,
                               user_id VARCHAR(3),
                               transportation_mode VARCHAR(30),
                               start_date_time DATETIME,
                               end_date_time DATETIME,
                               FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE)"""

trackpoint_table = """CREATE TABLE IF NOT EXISTS %s (
                                 id INT AUTO_INCREMENT PRIMARY KEY,
                                 activity_id INT,
                                 latitude DOUBLE,
                                 longitude DOUBLE,
                                 altitude INT,
                                 date_time DATETIME,
                                 FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE)"""

def main():
    processor = None
    try:
        processor = GeolifeDataProcessor()
        processor.create_table(table_name="User", table_definition=user_table)
        processor.create_table(table_name="Activity", table_definition=activity_table)
        processor.create_table(table_name="TrackPoint", table_definition=trackpoint_table)
        processor.list_tables()
        processor.display_table(table_name="User")
        processor.display_table(table_name="Activity")
        processor.display_table(table_name="TrackPoint")
        processor.insert_user_table()
        processor.retrieve_data(table_name="User")
        print(f"Processing .plt files...")
        processor.process_geolife_data(data_directory="dataset/Data")
        processor.display_top20_rows()

    except Exception as e:
        print(f"ERROR: Database operation failed - {e}")
    finally:
        if processor:
            processor.db_connector.close_connection()

if __name__ == '__main__':
    main()
