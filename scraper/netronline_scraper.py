import requests
from bs4 import BeautifulSoup
import csv
import os

URL = "https://publicrecords.netronline.com/state/CA.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
}

def get_county_links():
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    # Save raw HTML for inspection
    debug_path = os.path.abspath("netronline_debug.html")
    with open(debug_path, "w") as file:
        file.write(soup.prettify())
    print(f"✅ HTML debug file saved at: {debug_path}")

    # Check if the table exists before continuing
    table = soup.find("table", {"class": "counties"})
    if table is None:
        print("❌ Table with class 'counties' not found. Check the HTML structure.")
        return []

    rows = table.find_all("tr")[1:]  # skip header

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        county = cols[0].get_text(strip=True).replace(" County", "")
        assessor = cols[1].find("a")["href"] if cols[1].find("a") else None
        recorder = cols[2].find("a")["href"] if cols[2].find("a") else None
        tax = cols[3].find("a")["href"] if cols[3].find("a") else None

        data.append({
            "county": county,
            "assessor_url": assessor,
            "recorder_url": recorder,
            "tax_url": tax
        })

    return data

def save_to_csv(data, filename="data/netronline_ca_counties.csv"):
    if not data:
        print("⚠️ No data to write.")
        return
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["county", "assessor_url", "recorder_url", "tax_url"])
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    county_data = get_county_links()
    save_to_csv(county_data)
    print(f"✅ Scraped {len(county_data)} counties and saved to CSV.")

