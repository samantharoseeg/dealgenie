import geopandas as gpd
import pandas as pd

# Load the parcel shapefile
print("📂 Loading parcel shapefile...")
gdf = gpd.read_file("/Users/samanthagrant/Desktop/dealgenie/data/LACounty_Parcels.shp")

# Load the APNs
apns = pd.read_csv("/Users/samanthagrant/Desktop/dealgenie/data/lacity_apns.csv", dtype=str)["AIN"].dropna().tolist()[:10]

# Filter parcels for these APNs
filtered_gdf = gdf[gdf["AIN"].isin(apns)]

# Print AIN and TaxRateCit
print(filtered_gdf[["AIN", "TaxRateCit"]])