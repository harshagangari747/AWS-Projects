import json
import boto3
import os
import docxpy
from io import BytesIO
import io
from boto3.dynamodb.conditions import Key


s3 = boto3.client('s3')
bucket_name = os.getenv('BUCKET_NAME')

dynamodb = boto3.resource('dynamodb')
table_name = os.getenv('DYNAMODB_TABLE_NAME')
table = dynamodb.Table(table_name)



def lambda_handler(event, context):
    print(event)


    objectKey = event['resumeFile']

    response = s3.get_object(Bucket=bucket_name, Key=objectKey)
    file_content = response['Body'].read()

    jobId = event['jobId']

    tmp_dir = os.path.join('/tmp', jobId)
    os.makedirs(tmp_dir, exist_ok=True)

    tmp_path = os.path.join('/tmp',objectKey)
    with open(tmp_path, 'wb') as f:
        f.write(file_content)

    
    try:
        text = docxpy.process(tmp_path)
        print(text)

        job_details = table.query(
            KeyConditionExpression='jobId = :jobId',
            ExpressionAttributeValues={
                ':jobId': jobId
            }
        )

        print('job details', job_details)

        inputData = {}
        inputData['jobDetails'] = job_details
        inputData['resumeText'] = text
        inputData['resumeFile'] = objectKey
        inputData['jobId'] = jobId
        inputData['emailId'] = event['emailId']

        return inputData
        
    except Exception as e:
        print(e)
        return None

   