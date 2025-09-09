import boto3
import os
import json
from boto3.dynamodb.conditions import Key, Attr
import base64
from requests_toolbelt.multipart import decoder
from decimal import Decimal


dynamodb_client = boto3.resource('dynamodb')
table_name = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb_client.Table(table_name)

s3_client = boto3.client('s3', region_name='us-west-2')

bucket_name = os.environ['RESUME_BUCKET']

sfn_client = boto3.client('stepfunctions')

cors_headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
}


def lambda_handler(event, context):

    print('base 64', event['isBase64Encoded'])

    body = event.get('body',{})
    if body:
        if event.get('isBase64Encoded', False):
            body = base64.b64decode(body)
        elif isinstance(body, str):
            body = body.encode('utf-8')
    else:
        return response(400, 'No body in request')

    applicantData = resumefile = None

    content_type = event.get('headers', {}).get('Content-Type') or event.get('headers', {}).get('content-type')
    print('ctype', content_type)
    parser = decoder.MultipartDecoder(body, content_type)

    for part in parser.parts:
        disposition = part.headers.get(b"Content-Disposition", b"").decode()
        if 'name="applicantData"' in disposition:
            applicantData = json.loads(part.text, parse_float=Decimal)

        if 'name="resumeFile"' in disposition:
            print('part content')
            resumefile = part.content

    if not applicantData or not resumefile:
        return response(400, 'No applicationData or resumeFile in request')

    # upload resume to s3
    try:
        print('resumeFile',resumefile)
        objectKey = f"{applicantData['jobId']}/{applicantData['firstname']+applicantData['lastname']}.docx"
        print('objectKey', objectKey)
        s3_client.put_object(Body=resumefile, Bucket=bucket_name, Key=objectKey, ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        applicantData['resumeFile'] = objectKey
        
    except Exception as e:
        print(f'S3 Upload failure  because of {e}')
        return response(500, 'Error uploading resume to s3')

    # upload application data to dynamodb. PK jobId and SK emailId
    try:
        Key = {
        'jobId': applicantData['jobId'],
        'emailId': applicantData['emailId']
        }


        table.put_item(Item=applicantData)

    except Exception as e:
        print(f'DynamoDB put failure {applicantData} because of {e}')
        return response(500, 'Error uploading application data to dynamodb')

    # start step function
    try:
        sfn_client.start_execution(
            stateMachineArn=os.environ['STEP_FUNCTION_ARN'],
            input=json.dumps(applicantData, default=str)
        )
    except Exception as e:
        print(f'Step function failure {applicantData} because of {e}')
        return response(500, 'Error starting step function')

    return response(200, 'We received your application. Please give our team to review your application and update you on the same.')

def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': cors_headers,
        'body': json.dumps(body)
    }