import geopandas as gpd

# Load the city boundary shapefile
print("📂 Loading city boundary shapefile...")
city_boundary = gpd.read_file("/Users/samanthagrant/Desktop/dealgenie/data/City_Boundaries.shp")

# Apply the filter
city_boundary_la = city_boundary[(city_boundary["CITYNAME_A"] == "LOS ANGELES") | (city_boundary["CITY_COMM_"] == "LOS ANGELES") | (city_boundary["CITY_COMM_"].str.contains("LOS ANGELES", case=False, na=False))]

# Count the number of features
print(f"Number of city boundary features for LA: {len(city_boundary_la)}")

# Display the filtered features
print("Filtered features (CITYNAME_A, CITY_COMM_):")
print(city_boundary_la[["CITYNAME_A", "CITY_COMM_"]])

# Check how many features have "LOS ANGELES" in any relevant field
print("\nAll unique CITYNAME_A values:")
print(city_boundary["CITYNAME_A"].unique())

print("\nAll unique CITY_COMM_ values:")
print(city_boundary["CITY_COMM_"].unique())