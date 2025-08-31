import json
import boto3
import os
import urllib.request
import base64
from boto3.dynamodb.conditions import Key, Attr
import time
from decimal import Decimal, getcontext, Inexact, Rounded

# Set strict decimal context to trap rounding/inexact errors early
context = getcontext()
context.traps[Inexact] = True
context.traps[Rounded] = True

kinesis = boto3.client('kinesis')
kinesis_stream = os.environ['KINESIS_STREAM_NAME']

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['DYNAMODB_TABLE_NAME']

rev_geo_code_api_url = os.environ['REV_GEO_CODE_API_URL']

dynamodb_table = dynamodb.Table(table_name)

def lambda_handler(event, context):

    # check preflight request
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': ''
        }
    for record in event['Records']:
        print('record', record)
        decoded_bytes = base64.b64decode(record['kinesis']['data'])
        decoded_str = decoded_bytes.decode('utf-8')
        payload = json.loads(decoded_str)        
        trainId = payload['trainId']
        train_name = payload['trainName']

        curr_sequence_number = int(record['kinesis']['sequenceNumber'])
        last_arrival_time = record['kinesis']['approximateArrivalTimestamp']

        # Check if the record exists
        response = dynamodb_table.get_item(Key={'PK': str(trainId)})
        item = response.get('Item', None)

        if item:
            print('time stamps', item['approximateArrivalTimestamp'],'and',last_arrival_time)
            if item['lastSequenceNumber'] == curr_sequence_number or item['approximateArrivalTimestamp'] > last_arrival_time:
                print('Duplicate or older record... Skipping processing the record')
                return

        # Prepare payload
        payload['PK'] = str(trainId)
        payload['trainName'] = train_name
        payload['lastSequenceNumber'] = str(curr_sequence_number)
        payload['lastUpdatedTime'] = int(time.time())
        # Keep lat/lon as strings first, convert below
        payload['latitude'] = str(payload['latitude'])
        payload['longitude'] = str(payload['longitude'])
        payload['approximateArrivalTimestamp'] = int(last_arrival_time)

        # fetch address
        geo_coded_address = fetch_reverse_geocode(payload['latitude'], payload['longitude'])
        if not geo_coded_address:
            print('Unable to fetch address for the given co-ords')
            return

        geo_coded_address = json.loads(geo_coded_address)
        print('geo_coded address type', type(geo_coded_address), 'address', geo_coded_address['address'])
        payload['address'] = geo_coded_address['address']

        # Convert all floats (including nested) to Decimal
        payload = convert_floats_to_decimal(payload)

        # Validate no floats remain
        assert_no_floats(payload)

        if item:
            expr_vals = {
                ':val1': payload['lastSequenceNumber'],
                ':val2': payload['approximateArrivalTimestamp'],
                ':val3': payload['address'],
                ':val4': payload['lastUpdatedTime'],
                ':val5': payload['latitude'],
                ':val6': payload['longitude']
            }
            expr_vals = convert_floats_to_decimal(expr_vals)
            assert_no_floats(expr_vals)

            response = dynamodb_table.update_item(
                Key={'PK': trainId},
                UpdateExpression="set lastSequenceNumber = :val1, approximateArrivalTimestamp = :val2, address = :val3, lastUpdatedTime = :val4, latitude = :val5, longitude = :val6",
                ExpressionAttributeValues=expr_vals
            )
        else:
            print('Putting the payload in the db', payload)
            response = dynamodb_table.put_item(Item=payload)

        print('Record inserted', item)


    return {
        'statusCode': 200,
        'body': json.dumps(item, cls=DecimalEncoder)
    }


def convert_floats_to_decimal(obj):
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    elif isinstance(obj, float):
        print(f"Converting float to Decimal: {obj}")
        return Decimal(str(obj))
    else:
        return obj

def assert_no_floats(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            assert_no_floats(v)
    elif isinstance(obj, list):
        for i in obj:
            assert_no_floats(i)
    elif isinstance(obj, float):
        raise TypeError(f"Float value found in payload at leaf: {obj}")

def fetch_reverse_geocode(lat, lon):
    fq_url = f'{rev_geo_code_api_url}&lat={lat}&lon={lon}'

    print('api url', fq_url)
    req = urllib.request.Request(fq_url, headers={'Content-Type':'application/json'},method='GET')
    try:
        with urllib.request.urlopen(req) as response:
            result = response.read().decode()
            print('result from geocode', result)
            return result
    except Exception as e:
        print('api exception',e)
        return None

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)
