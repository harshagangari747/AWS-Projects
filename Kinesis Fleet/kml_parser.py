import json
from xml.etree import ElementTree as ET

# Read your KML file
filename = "input.kml" # ===> Update this
with open(filename, "r", encoding="utf-8") as f:
    kml_data = f.read()

# Parse the XML
ns = {'kml': 'http://www.opengis.net/kml/2.2'}
root = ET.fromstring(kml_data)
placemarks = root.findall(".//kml:Placemark", ns)

# Construct GeoJSON
geojson = {
    "type": "FeatureCollection",
    "features": []
}

for placemark in placemarks:
    name_elem = placemark.find("kml:name", ns)
    coord_elem = placemark.find(".//kml:coordinates", ns)

    if name_elem is not None and coord_elem is not None:
        name = name_elem.text
        coords_text = coord_elem.text.strip()
        lon, lat, *_ = map(float, coords_text.split(","))

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "name": name
            }
        }
        geojson["features"].append(feature)

# Write GeoJSON to file
with open("newdelhi-mumbai-rajdhani.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, indent=2)

print("Converted and saved as output.geojson")
