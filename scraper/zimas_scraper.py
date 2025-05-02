import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# File paths (using absolute paths)
INPUT_CSV = "/Users/samanthagrant/Desktop/dealgenie/data/lacity_apns.csv"
OUTPUT_CSV = "/Users/samanthagrant/Desktop/dealgenie/data/zimas_data.csv"
SKIPPED_CSV = "/Users/samanthagrant/Desktop/dealgenie/data/zimas_skipped.csv"

# Load APNs
try:
    print("📂 Loading APNs from CSV...")
    apns = pd.read_csv(INPUT_CSV, dtype=str)["AIN"].dropna().tolist()
    apns = apns[:10]  # Start with a small batch
    print(f"✅ Loaded {len(apns)} APNs for processing.")
except Exception as e:
    print(f"❌ Failed to load APNs from {INPUT_CSV}: {e}")
    exit()

# Setup Chrome
chrome_options = Options()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
driver = webdriver.Chrome(options=chrome_options)

results = []
skipped_apns = []

try:
    print("🌐 Navigating to ZIMAS...")
    driver.get("https://zimas.lacity.org/")
    time.sleep(5)

    # Accept Terms
    print("🕐 Waiting for Terms popup...")
    try:
        accept_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@value='Accept']"))
        )
        accept_button.click()
        print("✅ Accepted terms.")
        time.sleep(2)
    except Exception as e:
        print(f"⚠️ No terms modal found or failed to accept: {e}")

    for apn in apns:
        print(f"\n🔍 Scraping APN: {apn}")
        try:
            # Open the APN tab using JavaScript
            driver.execute_script("ZimasData.showSearchParameters('divSearchBodyAPN');")
            print("✅ Switched to Assessor Parcel No. tab.")
            time.sleep(2)

            # Wait for search input and ensure it's interactable
            search_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtAPN"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", search_input)
            time.sleep(0.5)
            search_input.clear()
            search_input.send_keys(apn)

            # Wait for search button and ensure it's interactable
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnSearchGo"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            search_button.click()
            print("🚀 Search submitted. Waiting for results...")

            # Wait for the results section to load or "No matching records" message
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "divSearchResults"))
            )
            time.sleep(10)  # Additional delay for dynamic content

            # Parse results
            soup = BeautifulSoup(driver.page_source, "html.parser")
            result = {
                "APN": apn,
                "Zoning Code": None,
                "Land Use Designation": None,
                "Height District": None,
                "TOC Tier": None,
                "Specific Plan Area": None,
                "Overlay Zones": [],
                "Historic Preservation Info": [],
                "Previous Zoning Cases": []
            }

            # Check for "No matching records" message
            no_records = soup.find("div", string=lambda text: text and "No matching records found" in text)
            if no_records:
                print(f"⚠️ No matching records found for APN {apn}")
                results.append(result)
                time.sleep(random.uniform(1, 3))
                continue

            # Find the data table
            data_table = soup.find("table", class_="DataClearDataTabs")
            if not data_table:
                print("⚠️ Data table not found in HTML")
                results.append(result)
                time.sleep(random.uniform(1, 3))
                continue

            # Extract data by iterating over table rows
            rows = data_table.find_all("tr")
            i = 0
            while i < len(rows) - 1:
                label_row = rows[i]
                value_row = rows[i + 1]
                label = label_row.find("td", class_="DataClearDataTabs")
                if label:
                    label_text = label.get_text(strip=True)
                    value = value_row.find("td", class_="DataClearDataTabs")
                    if value:
                        # Handle single-value fields
                        if label_text == "Zoning":
                            result["Zoning Code"] = value.get_text(strip=True)
                        elif label_text == "General Plan Land Use":
                            result["Land Use Designation"] = value.get_text(strip=True)
                        elif label_text == "Height District":
                            result["Height District"] = value.get_text(strip=True)
                        elif label_text == "TOC Tier":
                            result["TOC Tier"] = value.get_text(strip=True)
                        elif label_text == "Specific Plan":
                            result["Specific Plan Area"] = value.get_text(strip=True)
                        # Handle list-based fields
                        elif label_text in ["Overlay Zones", "Historic Preservation Overlay Zone (HPOZ)", "Case Numbers"]:
                            list_items = value.find_all("li")
                            values = [li.get_text(strip=True) for li in list_items] if list_items else [value.get_text(strip=True)]
                            if label_text == "Overlay Zones":
                                result["Overlay Zones"] = values
                            elif label_text == "Historic Preservation Overlay Zone (HPOZ)":
                                result["Historic Preservation Info"] = values
                            elif label_text == "Case Numbers":
                                result["Previous Zoning Cases"] = values
                i += 2

            # Log warnings for missing fields
            if not result["Zoning Code"]:
                print("⚠️ Zoning not found in HTML")
            if not result["Land Use Designation"]:
                print("⚠️ General Plan Land Use not found in HTML")
            if not result["Height District"]:
                print("⚠️ Height District not found in HTML")
            if not result["TOC Tier"]:
                print("⚠️ TOC Tier not found in HTML")
            if not result["Specific Plan Area"]:
                print("⚠️ Specific Plan not found in HTML")
            if not result["Overlay Zones"]:
                print("⚠️ Overlay Zones not found in HTML")
            if not result["Historic Preservation Info"]:
                print("⚠️ Historic Preservation not found in HTML")
            if not result["Previous Zoning Cases"]:
                print("⚠️ Case Numbers not found in HTML")

            results.append(result)
            print(f"✅ Collected: {result}")

            time.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f"❌ Failed for APN {apn}: {str(e)}")
            skipped_apns.append(apn)
            time.sleep(random.uniform(6, 12))

    # Save results
    if results:
        df = pd.DataFrame(results)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: "; ".join(x) if isinstance(x, list) else x)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"📁 Saved {len(results)} records to {OUTPUT_CSV}")
    else:
        print("⚠️ No data was collected.")

    # Save skipped APNs
    if skipped_apns:
        pd.DataFrame(skipped_apns, columns=["Skipped_APN"]).to_csv(SKIPPED_CSV, index=False)
        print(f"⚠️ Logged {len(skipped_apns)} skipped APNs to {SKIPPED_CSV}")

finally:
    driver.quit()
    print("🎉 Done!")