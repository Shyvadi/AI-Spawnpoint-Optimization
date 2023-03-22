import math
import pymysql
from shapely.geometry import Point, Polygon
from tqdm import tqdm
import numpy as np
import concurrent.futures


def process_grid_point(point, valid_spawnpoints, geofence, radius, min_spawnpoints):
    lat, lon = point
    if not point_within_geofence((lat, lon), geofence):
        return None

    nearby_spawnpoints = [(lat2, lon2) for lat2, lon2 in valid_spawnpoints
                          if haversine_distance((lat, lon), (lat2, lon2)) <= radius]

    if len(nearby_spawnpoints) >= min_spawnpoints:
        return lat, lon, nearby_spawnpoints

    return None


def create_grid(min_lat, max_lat, min_lon, max_lon, step):
    latitudes = np.arange(min_lat, max_lat, step)
    longitudes = np.arange(min_lon, max_lon, step)
    return [(lat, lon) for lat in latitudes for lon in longitudes]


# Step 1: Parse and load the spawnpoint data
def load_spawnpoints(database_config):
    spawnpoints = []

    # Connect to the database
    conn = pymysql.connect(
        database=database_config["database"],
        user=database_config["user"],
        password=database_config["password"],
        host=database_config["host"],
        port=database_config["port"]
    )

    # Fetch the spawnpoints data
    with conn.cursor() as cursor:
        cursor.execute("SELECT lat, lon FROM database.spawnpoint WHERE UNIX_TIMESTAMP() - 86400 < spawnpoint.updated")
        for row in cursor:
            # print(f"Row: {row}")
            lat, lon = row
            spawnpoints.append((float(lat), float(lon)))

    # Close the database connection
    conn.close()

    return spawnpoints

# Step 2: Calculate the distance between two coordinates
def haversine_distance(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371  # Earth radius in kilometers

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c * 1000  # Return distance in meters


# Step 3: Determine if a point is within a given geofence
def point_within_geofence(point, geofence):
    lat, lon = point
    geofence_polygon = Polygon(geofence)
    return geofence_polygon.contains(Point(lat, lon))


# Step 4: Find optimal locations for collecting N spawnpoints
def find_optimal_locations(spawnpoints, geofence, radius, min_spawnpoints, grid_density_factor=1):
    geofence_polygon = Polygon(geofence)
    valid_spawnpoints = [p for p in spawnpoints if point_within_geofence(p, geofence)]
    print(f"Number of valid spawnpoints within geofence: {len(valid_spawnpoints)}")

    min_lat, max_lat = min(p[0] for p in geofence), max(p[0] for p in geofence)
    min_lon, max_lon = min(p[1] for p in geofence), max(p[1] for p in geofence)

    # Create a grid with a step size (in degrees) corresponding to approximately 70m
    step_size = 70 / (6371000 * np.pi / 180)  # Convert meters to degrees

    # Adjust the step size based on the grid_density_factor
    step_size /= grid_density_factor

    grid_points = create_grid(min_lat, max_lat, min_lon, max_lon, step_size)

    optimal_locations = []
    used_spawnpoints = set()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_grid_point, grid_points, [valid_spawnpoints] * len(grid_points),
                                         [geofence] * len(grid_points), [radius] * len(grid_points),
                                         [min_spawnpoints] * len(grid_points)), total=len(grid_points),
                            desc="Processing grid points"))

    for result in results:
        if result is not None:
            lat, lon, nearby_spawnpoints = result
            unique_spawnpoints = [sp for sp in nearby_spawnpoints if sp not in used_spawnpoints]

            if len(unique_spawnpoints) >= min_spawnpoints:
                optimal_locations.append((lat, lon, unique_spawnpoints))
                used_spawnpoints.update(unique_spawnpoints)

    return optimal_locations


# Step 5: Adjust the geofence and N as needed
geofence = [(44.371398,-78.788005), (44.297078,-78.744307), (44.30785,-78.67876), (44.388749,-78.721002),(44.371398,-78.788005)]  # Define your geofence here
database_config = {
    "database": "database",
    "user": "user",
    "password": "password",
    "host": "host",
    "port": 3306
}
if __name__ == '__main__':
    spawnpoints = load_spawnpoints(database_config)
    radius = 70 / 2  # The radius of the circle is half the diameter (70m)
    min_spawnpoints = 11  # Set the minimum number of spawnpoints to collect

    # Find the optimal locations to collect at least min_spawnpoints spawnpoints
    optimal_locations = find_optimal_locations(spawnpoints, geofence, radius, min_spawnpoints, grid_density_factor=3)


    # Print the results
    print(f"Found {len(optimal_locations)} optimal locations for collecting at least {min_spawnpoints} spawnpoints each.")
    for i, (lat, lon, nearby_spawnpoints) in enumerate(optimal_locations, 1):
        print(f"Optimal Location {i}: Latitude {lat}, Longitude {lon}, Spawnpoints: {len(nearby_spawnpoints)}")

