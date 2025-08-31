import json
import os
import boto3
from decimal import Decimal

cors_headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
}

dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)


def lambda_handler(event, context):
    print('event', event)
    # get trainId request params
    query_string = event['queryStringParameters']
    if not query_string:
        return response(400, 'Missing query string parameters')

    print('queryparams',query_string)
    trainId = query_string.get('trainId')
    if not trainId:
        return response(400, 'Missing trainId or trainName parameter')

    # get item from dynamodb
    try:
        db_response = table.get_item(Key={'PK': trainId})
        print('response',db_response)
        item = db_response['Item']


        if item:
            return response(200, item)
    except Exception as e:
        print('response error',e)
        return response(200, {'message':'Status Not found'})


def response(statusCode, body):
    return {
        'statusCode': statusCode,
        'body': json.dumps(body,cls=DecimalEncoder),
        'headers': cors_headers
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)