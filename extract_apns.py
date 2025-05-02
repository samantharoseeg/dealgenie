import geopandas as gpd
import pandas as pd

# Load the LA County parcel shapefile
shapefile_path = "data/LACounty_Parcels_Shapefile/LACounty_Parcels.shp"
gdf = gpd.read_file(shapefile_path)

# Drop rows where the APN (AIN) is missing
gdf = gdf[gdf["AIN"].notna()]

# Clean up: ensure AINs are strings and zero-padded to 10 digits
gdf["AIN"] = gdf["AIN"].astype(str).str.zfill(10)

# Save to CSV
output_path = "data/la_apns.csv"
gdf[["AIN"]].to_csv(output_path, index=False)

print(f"✅ Saved {len(gdf)} APNs to {output_path}")
