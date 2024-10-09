from DbConnector import DbConnector
from tabulate import tabulate
import os

class GeolifeTask:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def load_labels_for_user(self, label_file_path):
        labels = []
        with open(label_file_path, 'r') as f:
            next(f)  # Skip header line
            for line in f:
                start_time, end_time, transport_mode = line.strip().split('\t')
                labels.append({
                    'start_time': start_time.replace('/', '-'),  
                    'end_time': end_time.replace('/', '-'),
                    'transport_mode': transport_mode
                })
        return labels

    def process_user_data(self, dataset_path):
        # Loop through each user folder
        for root, dirs, files in os.walk(dataset_path):
            user_id = os.path.basename(root)
            
            # Only proceed if the current folder looks like a valid user directory (e.g., folder names '000', '001', etc.)
            if user_id.isdigit():
                print(f"Processing user_id: {user_id}")

                # Check if the user already exists in the User table
                self.cursor.execute("SELECT id FROM User WHERE id = %s", (user_id,))
                result = self.cursor.fetchone()

                # If the user is not already in the database, insert them
                if result is None:
                    # Check if labels.txt exists for this user
                    label_file_path = os.path.join(root, 'labels.txt')
                    has_label = os.path.exists(label_file_path)
                    
                    # Insert the user into the database
                    self.insert_user(user_id, has_label)

                    # Load transportation mode labels if they exist
                    labels = self.load_labels_for_user(label_file_path) if has_label else []
                    if has_label:
                        print(f"Loaded {len(labels)} labels for user {user_id}")

                    # Process each .plt file in the user's Trajectory folder
                    for file in files:
                        if file.endswith(".plt"):
                            file_path = os.path.join(root, file)
                            print(f"Processing file: {file_path}")
                            
                            # Process .plt file and insert data into Activity and TrackPoint tables
                            self.process_plt_file(file_path, user_id, labels)

    def process_plt_file(self, file_path, user_id, labels):
        trackpoints = []
        with open(file_path, 'r') as f:
            lines = f.readlines()[6:] 
            if len(lines) == 0 or len(lines) > 2500:  
                print(f"Skipping file {file_path} due to too many trackpoints")
                return
            
            first_line = lines[0].strip().split(',')
            last_line = lines[-1].strip().split(',')

            start_datetime = f"{first_line[5]} {first_line[6]}"
            end_datetime = f"{last_line[5]} {last_line[6]}"

            transport_mode = self.match_transportation_mode(start_datetime, end_datetime, labels)

            activity_id = self.insert_activity(user_id, transport_mode, start_datetime, end_datetime)
            if activity_id:
                print(f"Inserted activity with id: {activity_id}")

            # Collect all trackpoints for batch insert
            for line in lines:
                data = line.strip().split(',')
                latitude = float(data[0])
                longitude = float(data[1])
                altitude = float(data[3])
                date = data[5]
                time = data[6]
                date_time = f"{date} {time}"

                trackpoints.append((activity_id, latitude, longitude, altitude, date_time))
        
        # Perform batch insert
        self.insert_trackpoints_batch(trackpoints)

    def match_transportation_mode(self, start_datetime, end_datetime, labels):
        for label in labels:
            if label['start_time'] == start_datetime and label['end_time'] == end_datetime:
                return label['transport_mode']
        return None  # No match

    def create_table(self):
        # User table
        user_query = """
        CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(10) PRIMARY KEY,
                   has_label BOOLEAN
                );
                """
        self.cursor.execute(user_query)
        
        # Activity table
        activity_table_query = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            user_id VARCHAR(10), 
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
        print(f"Inserted user {user_id} with label {has_label}")
        self.db_connection.commit()

    # Insert activity data into the Activity table
    def insert_activity(self, user_id, transport_mode, start_datetime, end_datetime):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(query, (user_id, transport_mode, start_datetime, end_datetime))
        self.db_connection.commit()
        return self.cursor.lastrowid  

    # Batch insert trackpoints into TrackPoint table
    def insert_trackpoints_batch(self, trackpoints):
        query = """
        INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.executemany(query, trackpoints)
        self.db_connection.commit()

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
        
        # Step 1: Create the required tables (User, Activity, TrackPoint)
        program.create_table()  

        # Step 2: Process and insert data
        dataset_path = "dataset/Data"  # Update this to your actual path based on your system
        
        # Process all user data and insert into the database.
        program.process_user_data(dataset_path)

        # Step 3: Fetch and show the inserted data from the tables (optional for debugging)
        print("Data from User table:")
        program.fetch_data("User")
        
        print("Data from Activity table:")
        program.fetch_data("Activity")
        
        print("Data from TrackPoint table:")
        program.fetch_data("TrackPoint")
        
        # Show the current state of the tables
        program.show_tables()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
