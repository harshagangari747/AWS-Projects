import json

import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

cors_headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
}

dynamodb_client = boto3.resource('dynamodb')

table_name = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb_client.Table(table_name)

def lambda_handler(event, context):

    body = json.loads(event['body'])
    job_details = body['job_details']

    try:
        if not job_details['jobId']:
            return response(400, 'Job ID is required')
            
        result = table.put_item(
            Item=job_details
        )

        return response(200, 'Vacancy posted successfully')
    except Exception as e:
        print(e)
        return response(500, 'Error saving job details')


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': cors_headers,
        'body': json.dumps(body)
    }
