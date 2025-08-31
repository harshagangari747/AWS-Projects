import json
import time
import random
import requests
import argparse
import sys

# CONFIGURATION
API_ENDPOINT = 'https://hm4p97it6a.execute-api.ap-south-1.amazonaws.com/test/location'      # Replace with your actual API endpoint
HTTP_METHOD = 'POST'                                # or 'GET'

# Function to extract coordinates from GeoJSON
def extract_coordinates(geojson_path):
    with open(geojson_path, 'r') as f:
        data = json.load(f)

    features = data.get('features', [])
    coords = []

    for feature in features:
        geometry = feature.get('geometry', {})
        if geometry['type'] == 'LineString':
            coords.extend(geometry['coordinates'])
        elif geometry['type'] == 'Point':
            coords.append(geometry['coordinates'])
        else:
            print(f"Unsupported geometry type: {geometry['type']}")

    return coords

# Function to call API with a lat/lon point
def send_location(payload):

    try:
        if HTTP_METHOD.upper() == 'POST':
            response = requests.post(API_ENDPOINT, json=payload)
        else:
            response = requests.get(API_ENDPOINT, params=payload)

        print(f"Sent: {payload} | Status Code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error sending data: {e}")

# Main loop
def simulate_train_movement(args):
    GEOJSON_FILE = args.file
    coords = extract_coordinates(GEOJSON_FILE)

    if not coords:
        print("No coordinates found.")
        return
    
    train_id = args.train_id
    train_name = args.name
    train_source = args.source
    train_destination = args.destination
    train_journey_date = args.date
    train_departure_time = args.dept_time
    train_arrival_time = args.arr_time
    obj ={}
    obj['trainId'] = train_id
    obj['trainName'] = train_name
    obj['trainSource'] = train_source
    obj['trainDestination'] = train_destination
    obj['journeyDate'] = train_journey_date
    obj['departureTime'] = train_departure_time
    obj['arrivalTime'] = train_arrival_time
    for lon, lat in coords:  # GeoJSON format is [longitude, latitude]

        obj['latitude'] = lat
        obj['longitude'] = lon
        send_location(obj)
        delay = random.randint(5,7)
        print(f"Waiting {delay} seconds before next location...")
        time.sleep(delay)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Simulate train movement from a GeoJSON file.")
    parser.add_argument('-f', '--file', required=True, help='Path to the GeoJSON file')
    parser.add_argument('-tid','--train-id',required=True, help='Train id')
    parser.add_argument('-n','--name',required=True,help='Train name')
    parser.add_argument('-s','--source',required=True,help='Train departure station')
    parser.add_argument('-d','--destination',required=True,help='Train arrival station')
    parser.add_argument('--date',required=True,help='Date of journey')
    parser.add_argument('--dept-time',required=True, help='Departure Time')
    parser.add_argument('--arr-time',required=True,help='Train expected arrival time')


    args = parser.parse_args()

    print('args',args)
    
    simulate_train_movement(args)
