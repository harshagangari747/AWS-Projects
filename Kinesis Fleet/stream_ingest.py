import json
import boto3
import os

kinesis = boto3.client('kinesis')
KINESIS_STREAM = os.environ['KINESIS_STREAM_NAME']

cors_headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'OPTIONS,POST'
}

def lambda_handler(event, context):
    # TODO implement
    print('stream_ingest: event',event)
    body = event.get('body',{})

    try:
        body = json.loads(body)
        trainId = body.get('trainId', '')
        if not body:
            return response(400, 'body is required')
        responses = kinesis.put_records(
            StreamName=KINESIS_STREAM,
            Records=[
                {
                    'Data': json.dumps(body),
                    'PartitionKey':  trainId
                }
            ]
        )
        print('kinesis response',responses)
    except Exception as e:
        print('stream_ingest: error', e)
        return response(500, 'error')

    return response(200, 'success') 

def response(status_code, body):
    return {
        'statusCode': status_code,
        'body': json.dumps(body),
        'headers': cors_headers
    }
