from DbConnector import DbConnector
from tabulate import tabulate
import dataset
import os

class GeolifeTask:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        
    def process_user_data(self, dataset_path):
        # Loop through each user folder
        for user_folder in os.listdir(dataset_path):
            user_path = os.path.join(dataset_path, user_folder, 'Trajectory')
            
            if os.path.isdir(user_path):
                # Process each .plt file in the Trajectory folder
                for plt_file in os.listdir(user_path):
                    if plt_file.endswith(".plt"):
                        file_path = os.path.join(user_path, plt_file)
                        self.process_plt_file(file_path, user_folder)
                        
    def process_plt_file(self, file_path, user_id):
        trackpoints = []
        with open(file_path, 'r') as f:
            lines = f.readlines()[6:]  # Skip the first 6 lines (headers)
            if len(lines) > 2500:  # Skip activities with more than 2500 trackpoints
                return
            for line in lines:
                data = line.strip().split(',')
                latitude = float(data[0])
                longitude = float(data[1])
                altitude = int(data[3])
                date = data[5]
                time = data[6]
                date_time = f"{date} {time}"

                # Collect trackpoints for batch insert later
                trackpoints.append((user_id, latitude, longitude, altitude, date_time))
        
        # Perform batch insert
        self.insert_trackpoints_batch(trackpoints)

    def create_table(self):
        # User table
        user_query = """
        CREATE TABLE IF NOT EXISTS User (
                   id INT PRIMARY KEY,
                   has_label BOOLEAN
                );
                """
        self.cursor.execute(user_query)
        
        # Activity table
        activity_table_query = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            user_id INT, 
            transportation_mode VARCHAR(50), 
            start_date_time DATETIME, 
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
        );
        """
        self.cursor.execute(activity_table_query)

        # TrackPoint table
        trackpoint_table = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
        id INT AUTO_INCREMENT PRIMARY KEY,
        activity_id INT NOT NULL,
        latitude FLOAT(10, 6),
        longitude FLOAT(10, 6),
        altitude INT,
        date_time DATETIME,
        FOREIGN KEY (activity_id) REFERENCES Activity(id)
    );
    """
        self.cursor.execute(trackpoint_table)
        print("Tables created successfully")

    # Insert user data into the User table
    def insert_user(self, user_id, has_label):
        query = "INSERT INTO User (id, has_label) VALUES (%s, %s)"
        self.cursor.execute(query, (user_id, has_label))

    # Insert activity data into the Activity table
    def insert_activity(self, user_id, transport_mode, start_datetime, end_datetime):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(query, (user_id, transport_mode, start_datetime, end_datetime))
        return self.cursor.lastrowid  # Return the generated activity ID

    # Batch insert trackpoints into TrackPoint table
    def insert_trackpoints_batch(self, trackpoints):
        query = """
        INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.executemany(query, trackpoints)

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s" % table_name
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s" % table_name
        self.cursor.execute(query)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = GeolifeTask()
        program.create_table()  # Removed the incorrect table_name parameter
        # Add your user processing logic here
        # Example: program.process_user_data("path_to_dataset")

        # Fetch and show tables after inserting data
        program.show_tables()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
