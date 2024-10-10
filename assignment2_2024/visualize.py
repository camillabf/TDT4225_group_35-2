import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

user_id = '128'  # Change to the desired user ID
user_data_path = f"dataset/Data/{user_id}/Trajectory"

def load_plt_data(path):
    data = []
    for filename in os.listdir(path):
        if filename.endswith('.plt'):
            file_path = os.path.join(path, filename)
            with open(file_path, 'r') as f:
                # Skip the first 6 lines
                for _ in range(6):
                    f.readline()
                for line in f:
                    parts = line.strip().split(',')
                    lat = float(parts[0])  # Latitude
                    lon = float(parts[1])  # Longitude
                    alt = float(parts[3])  # Altitude
                    data.append([lon, lat, alt])
    return pd.DataFrame(data, columns=['lon', 'lat', 'alt'])

# Load the data for the specified user
df = load_plt_data(user_data_path)

plt.figure(figsize=(10, 8))
scatter = plt.scatter(df['lon'], df['lat'], c=df['alt'], cmap='viridis', s=1)
plt.colorbar(scatter, label='Altitude (meters)')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Geolife Dataset: Latitude vs. Longitude Colored by Altitude')
plt.show()
