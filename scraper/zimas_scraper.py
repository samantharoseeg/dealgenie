#!/usr/bin/env python3
import os
import csv
import logging
import requests
import time

# — automatically find files in the same folder as this script —
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
APNS_CSV_PATH = os.path.join(SCRIPT_DIR, "apns.csv")
OUTPUT_CSV_PATH = os.path.join(SCRIPT_DIR, "zimas_data.csv")

# — LA County GIS REST endpoint for parcel features —
GIS_URL = (
    "https://public.gis.lacounty.gov/public/rest/services/"
    "LACounty_Cache/LACounty_Parcel/MapServer/0/query"
)

def load_apns_from_csv(path=APNS_CSV_PATH):
    """Load APNs from a simple one-column CSV."""
    apns = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                apns.append(row[0].strip())
    return apns

def fetch_parcel_data(apn):
    """
    Query LA County’s public GIS for parcel attributes by APN (using the AIN field).
    Returns the attributes dict or None.
    """
    logging.info(f"Querying GIS for APN: {apn}")
    params = {
        "where": f"AIN = '{apn}'",
        "outFields": "*",
        "f": "json"
    }
    resp = requests.get(GIS_URL, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.warning(f"No GIS data for {apn}: {e}")
        return None

    data = resp.json()
    features = data.get("features", [])
    if not features:
        logging.warning(f"No features returned for {apn}")
        return None

    return features[0]["attributes"]

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    apns = load_apns_from_csv()
    if not apns:
        logging.error("No APNs found in apns.csv. Exiting.")
        return

    # Prepare output CSV
    fieldnames = ["APN", "Address", "UseDescription", "UseCode"]
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        # Loop through every APN, fetch and write
        for apn in apns:
            attrs = fetch_parcel_data(apn)
            if attrs:
                writer.writerow({
                    "APN": apn,
                    "Address": attrs.get("SitusFullAddress", ""),
                    "UseDescription": attrs.get("UseDescription", ""),
                    "UseCode": attrs.get("UseCode", "")
                })
            # pause so we don't hammer the server
            time.sleep(0.2)

    logging.info(f"Done! Results written to {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
