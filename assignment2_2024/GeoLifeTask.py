from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os

class Task1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    def create_table(self, table_name, query):
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(f"Data from table {table_name}, raw format:")
        print(rows)
        print(f"Data from table {table_name}, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print(f"Dropping table {table_name}...")
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        self.db_connection.commit()
        print(tabulate(rows, headers=self.cursor.column_names))

    def describe_table(self, table_name):
        query = "DESCRIBE %s"
        self.cursor.execute(query % table_name)
        content = self.cursor.fetchall()
        print(tabulate(content))

    def fill_user_table(self):
        insert_query = """INSERT INTO User (id, has_label) VALUES (%s, %s)"""
        check_query = """SELECT COUNT(*) FROM User WHERE id = %s"""
        update_query = """UPDATE User SET has_label = true WHERE id = %s"""
        
        for user in range(182):
            user_id = f"{user:03d}"
            self.cursor.execute(check_query, (user_id,))
            count = self.cursor.fetchone()[0]

            if count == 0:
                self.cursor.execute(insert_query, (user_id, False))
                self.db_connection.commit()

        with open(r"dataset/labeled_ids.txt", 'r') as f:
            labeled_users = [line.strip() for line in f]
            self.cursor.executemany(update_query, [(user,) for user in labeled_users])
            self.db_connection.commit()

    def insert_into_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        self.db_connection.commit()
        return self.cursor.lastrowid
    
    def insert_trackpoints_batch(self, trackpoints, batch_size=1000):
        query = """
        INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        for i in range(0, len(trackpoints), batch_size):
            batch = trackpoints[i:i+batch_size]
            self.cursor.executemany(query, batch)
        self.db_connection.commit()

    def read_labels_file(self, labels_file):
        labels = []
        with open(labels_file, 'r') as file:
            next(file)
            for line in file:
                start_time, end_time, mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')  
                end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')  
                labels.append((start_time, end_time, mode))
        return labels
    
    def process_plt_file(self, file_path, user_id, labels):
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()[6:]
                if not lines or len(lines) > 2500:
                    print(f"Skipping file {file_path} due to trackpoint limits.")
                    return

                start_line = lines[0].strip().split(',')
                end_line = lines[-1].strip().split(',')

                start_date_time = datetime.strptime(f"{start_line[5]} {start_line[6]}", '%Y-%m-%d %H:%M:%S')
                end_date_time = datetime.strptime(f"{end_line[5]} {end_line[6]}", '%Y-%m-%d %H:%M:%S')

                matched_label = next((label[2] for label in labels if label[0] == start_date_time and label[1] == end_date_time), None)
                if matched_label is None:
                    print(f"No matching label found for {file_path}")
                    return

                activity_id = self.insert_into_activity(user_id, matched_label, start_date_time, end_date_time)

                trackpoints = []
                for line in lines:
                    lat, lon, _, altitude, _, date, time = line.strip().split(',')
                    date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                    trackpoints.append((activity_id, float(lat), float(lon), int(float(altitude)), date_time))

                self.insert_trackpoints_batch(trackpoints)

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    def process_geolife_dataset(self, dataset_path):
        for user_id in os.listdir(dataset_path):
            user_folder = os.path.join(dataset_path, user_id, 'Trajectory')
            labels_file = os.path.join(dataset_path, user_id, 'labels.txt')

            labels = self.read_labels_file(labels_file) if os.path.exists(labels_file) else []

            if os.path.isdir(user_folder):
                for plt_file in os.listdir(user_folder):
                    if plt_file.endswith('.plt'):
                        file_path = os.path.join(user_folder, plt_file)
                        self.process_plt_file(file_path, user_id, labels)
    
    def show_top_rows(self):
        tables = ['User', 'Activity', 'TrackPoint']
        for table in tables:
            query = f"SELECT * FROM {table} LIMIT 20;"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            print(f"Top rows from {table}:")
            print(tabulate(rows, headers=self.cursor.column_names))
            print()

user_table = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(3) NOT NULL PRIMARY KEY,
                has_label BOOLEAN)
                """
                
activity_table = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(3),
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE)
                    """

                
trackpoint_table = """CREATE TABLE IF NOT EXISTS %s 
                    (id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    altitude INT,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE)
                    """
    


def main():
    program = None
    try:
        program = Task1()
        program.create_table(table_name="User", query=user_table)
        program.create_table(table_name="Activity", query=activity_table)
        program.create_table(table_name="TrackPoint", query=trackpoint_table)
        program.show_tables()
        program.describe_table(table_name="User")
        program.describe_table(table_name="Activity")
        program.describe_table(table_name="TrackPoint")
        program.fill_user_table()
        program.fetch_data(table_name="User")
        program.process_geolife_dataset(dataset_path="dataset/Data")
        program.show_top_rows()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
