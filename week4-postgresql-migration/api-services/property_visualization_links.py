#!/usr/bin/env python3
"""
PROPERTY VISUALIZATION LINKS GENERATOR
Add external map visualization links to property API responses
"""

import urllib.parse
from typing import Dict, Optional, Tuple
import psycopg2
import json
from dataclasses import dataclass

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

@dataclass
class VisualizationLinks:
    """Container for property visualization links"""
    google_maps: str
    google_earth: str
    openstreetmap: str
    apple_maps: str

class PropertyVisualizationGenerator:
    """Generate external visualization links for properties"""

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False

    def close_database(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def generate_google_maps_link(self, latitude: float, longitude: float, address: str = None) -> str:
        """Generate Google Maps link with satellite view"""
        base_url = "https://www.google.com/maps"

        if address:
            # Use address search if available
            query = urllib.parse.quote(address)
            return f"{base_url}/search/{query}/@{latitude},{longitude},18z/data=!3m1!1e3"
        else:
            # Use coordinates directly
            return f"{base_url}/@{latitude},{longitude},18z/data=!3m1!1e3"

    def generate_google_earth_link(self, latitude: float, longitude: float) -> str:
        """Generate Google Earth Web link"""
        return f"https://earth.google.com/web/@{latitude},{longitude},150a,1000d,35y,0h,0t,0r"

    def generate_openstreetmap_link(self, latitude: float, longitude: float) -> str:
        """Generate OpenStreetMap link"""
        return f"https://www.openstreetmap.org/?mlat={latitude}&mlon={longitude}&zoom=18"

    def generate_apple_maps_link(self, latitude: float, longitude: float) -> str:
        """Generate Apple Maps link (works on iOS/macOS)"""
        return f"https://maps.apple.com/?ll={latitude},{longitude}&z=18&t=s"

    def generate_all_links(self, latitude: float, longitude: float, address: str = None) -> VisualizationLinks:
        """Generate all visualization links for a property"""
        return VisualizationLinks(
            google_maps=self.generate_google_maps_link(latitude, longitude, address),
            google_earth=self.generate_google_earth_link(latitude, longitude),
            openstreetmap=self.generate_openstreetmap_link(latitude, longitude),
            apple_maps=self.generate_apple_maps_link(latitude, longitude)
        )

    def get_property_with_links(self, apn: str) -> Optional[Dict]:
        """Retrieve property data with visualization links"""
        if not self.connect_database():
            return None

        try:
            # Get property data
            self.cursor.execute("""
            SELECT apn, site_address, latitude, longitude,
                   total_assessed_value, building_class, zoning_code,
                   assessed_land_val_numeric, assessed_improvement_val_numeric
            FROM unified_property_data
            WHERE apn = %s;
            """, (apn,))

            result = self.cursor.fetchone()
            if not result:
                return None

            apn, address, lat, lon, assessed_value, building_class, zoning, land_value, improvement_value = result

            if lat is None or lon is None:
                return {
                    "apn": apn,
                    "error": "No coordinates available for visualization"
                }

            # Generate visualization links
            links = self.generate_all_links(lat, lon, address)

            return {
                "apn": apn,
                "site_address": address,
                "latitude": lat,
                "longitude": lon,
                "total_assessed_value": assessed_value,
                "building_class": building_class,
                "zoning_code": zoning,
                "land_value": land_value,
                "improvement_value": improvement_value,
                "visualization_links": {
                    "google_maps": links.google_maps,
                    "google_earth": links.google_earth,
                    "openstreetmap": links.openstreetmap,
                    "apple_maps": links.apple_maps
                }
            }

        except Exception as e:
            print(f"❌ Error retrieving property {apn}: {e}")
            return None
        finally:
            self.close_database()

    def get_nearby_properties_with_links(self, latitude: float, longitude: float,
                                       radius_meters: float = 1000, limit: int = 10) -> list:
        """Get nearby properties with visualization links"""
        if not self.connect_database():
            return []

        try:
            # Use optimized geometry query with bounding box pre-filter
            self.cursor.execute("""
            SELECT apn, site_address, latitude, longitude,
                   total_assessed_value, building_class,
                   ST_Distance(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) * 111320 as distance_meters
            FROM unified_property_data
            WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            LIMIT %s;
            """, (longitude, latitude, longitude, latitude, radius_meters/111320,
                  longitude, latitude, radius_meters/111320, longitude, latitude, limit))

            results = self.cursor.fetchall()
            properties = []

            for result in results:
                apn, address, lat, lon, assessed_value, building_class, distance = result

                # Generate visualization links
                links = self.generate_all_links(lat, lon, address)

                properties.append({
                    "apn": apn,
                    "site_address": address,
                    "latitude": lat,
                    "longitude": lon,
                    "total_assessed_value": assessed_value,
                    "building_class": building_class,
                    "distance_meters": round(distance, 2),
                    "visualization_links": {
                        "google_maps": links.google_maps,
                        "google_earth": links.google_earth,
                        "openstreetmap": links.openstreetmap,
                        "apple_maps": links.apple_maps
                    }
                })

            return properties

        except Exception as e:
            print(f"❌ Error getting nearby properties: {e}")
            return []
        finally:
            self.close_database()

    def generate_html_report(self, properties: list, title: str = "Property Visualization Report") -> str:
        """Generate HTML report with clickable visualization links"""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .property {{ border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }}
        .property h3 {{ margin: 0 0 10px 0; color: #333; }}
        .links {{ margin: 10px 0; }}
        .links a {{
            display: inline-block;
            margin: 5px 10px 5px 0;
            padding: 8px 15px;
            background: #007bff;
            color: white;
            text-decoration: none;
            border-radius: 3px;
            font-size: 12px;
        }}
        .links a:hover {{ background: #0056b3; }}
        .google-maps {{ background: #4285f4; }}
        .google-earth {{ background: #34a853; }}
        .openstreetmap {{ background: #7ebc6f; }}
        .apple-maps {{ background: #000; }}
        .info {{ color: #666; font-size: 14px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p>Found {len(properties)} properties with visualization links</p>
"""

        for prop in properties:
            links = prop.get('visualization_links', {})
            assessed_value = prop.get('total_assessed_value', 0)
            distance = prop.get('distance_meters')

            html += f"""
    <div class="property">
        <h3>APN: {prop.get('apn', 'N/A')}</h3>
        <div class="info">
            <strong>Address:</strong> {prop.get('site_address', 'N/A')}<br>
            <strong>Coordinates:</strong> ({prop.get('latitude', 'N/A')}, {prop.get('longitude', 'N/A')})<br>
            <strong>Assessed Value:</strong> ${assessed_value:,} <br>
            <strong>Building Class:</strong> {prop.get('building_class', 'N/A')}
"""

            if distance is not None:
                html += f"            <br><strong>Distance:</strong> {distance}m"

            html += """
        </div>
        <div class="links">
            <strong>View Site:</strong>
"""

            if links.get('google_maps'):
                html += f'            <a href="{links["google_maps"]}" class="google-maps" target="_blank">Google Maps</a>\n'

            if links.get('google_earth'):
                html += f'            <a href="{links["google_earth"]}" class="google-earth" target="_blank">Google Earth</a>\n'

            if links.get('openstreetmap'):
                html += f'            <a href="{links["openstreetmap"]}" class="openstreetmap" target="_blank">OpenStreetMap</a>\n'

            if links.get('apple_maps'):
                html += f'            <a href="{links["apple_maps"]}" class="apple-maps" target="_blank">Apple Maps</a>\n'

            html += """        </div>
    </div>
"""

        html += """
</body>
</html>
"""
        return html

def test_visualization_links():
    """Test visualization link generation with real property data"""
    print("🔗 TESTING PROPERTY VISUALIZATION LINKS")
    print("=" * 60)

    generator = PropertyVisualizationGenerator()

    # Test specific APNs mentioned in the request
    test_apns = ["4306026007", "2004001003"]

    for apn in test_apns:
        print(f"\n--- Testing APN: {apn} ---")

        property_data = generator.get_property_with_links(apn)

        if property_data:
            if 'error' in property_data:
                print(f"❌ {property_data['error']}")
            else:
                print(f"✅ Property Found: {property_data['site_address']}")
                print(f"   Coordinates: ({property_data['latitude']}, {property_data['longitude']})")
                print(f"   Assessed Value: ${property_data['total_assessed_value']:,}")
                print("\n   Visualization Links:")
                links = property_data['visualization_links']
                print(f"   Google Maps: {links['google_maps']}")
                print(f"   Google Earth: {links['google_earth']}")
                print(f"   OpenStreetMap: {links['openstreetmap']}")
                print(f"   Apple Maps: {links['apple_maps']}")
        else:
            print(f"❌ Property not found: {apn}")

    # Test nearby search with visualization links
    print(f"\n--- Testing Nearby Search (Downtown LA) ---")
    downtown_lat, downtown_lon = 34.0522, -118.2437

    nearby_properties = generator.get_nearby_properties_with_links(
        downtown_lat, downtown_lon, radius_meters=1000, limit=5
    )

    print(f"Found {len(nearby_properties)} nearby properties with links")

    for i, prop in enumerate(nearby_properties, 1):
        print(f"\n{i}. APN: {prop['apn']}")
        print(f"   Address: {prop['site_address']}")
        print(f"   Distance: {prop['distance_meters']}m")
        print(f"   Google Maps: {prop['visualization_links']['google_maps']}")

    # Generate HTML report
    print(f"\n--- Generating HTML Report ---")
    html_content = generator.generate_html_report(
        nearby_properties,
        "Downtown LA Properties - Visualization Report"
    )

    with open('/Users/samanthagrant/Desktop/dealgenie/property_visualization_report.html', 'w') as f:
        f.write(html_content)

    print(f"✅ HTML Report saved: property_visualization_report.html")
    print(f"   Open in browser to test clickable links")

if __name__ == "__main__":
    test_visualization_links()