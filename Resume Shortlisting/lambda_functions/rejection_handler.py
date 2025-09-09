import json
import boto3
import os
import smtplib
from boto3.dynamodb.conditions import Key, Attr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from_email_address = os.environ['FROM_EMAIL_ADDRESS']
app_password = os.environ['EMAIL_PASSWORD']

bucket_name = os.environ['BUCKET_NAME']
dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']

s3 = boto3.client('s3')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

def lambda_handler(event, context):

    emailId = event['emailId']
    jobId = event['jobId']
    resumeFile = event['resumeFile']

    try:
        # send email
        send_email(jobId, emailId)

        # tag resume for expiration
        tag_resume_for_expiration(jobId, resumeFile)

        # update job status in dynamodb table
        update_job_status(jobId, emailId)
        
    except Exception as e:
        print('Error occurred: ', e)

    print(f'Completed rejection process successfully for candidate {emailId} for job {jobId}')

def send_email(jobId, receiver_email):
    sender_email = from_email_address
    print('sending reject email to', receiver_email)

    subject = f"Update on you recent application! {jobId}"
    body = (
        f" <p>Thank you for applying for Software Engineer role at ABC Company.</p>"
        f" <p> We are regretting to inform you that after careful consideration of your application, we are moving forward with candidates who"
        f" matches closely with the role workload. </p> <br/>"
        )

    # Compose the email
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    html_content = f"""
    <html>
    <body>
    <p>{body}</p>
    <p>Thank you once again for applying to ABC Compabny,<br>ABC Company</p>
    </body>
    </html>"""
    
    message.attach(MIMEText(html_content, "html"))
    

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        
    except Exception as e:
        raise Exception('Error occurred while sending email: ', e)

def tag_resume_for_expiration(jobId, resumeFile):
    objectKey = f'{jobId}/{resumeFile}'


    print('resumefile', resumeFile, 'jobId', jobId)
    try:
        s3.put_object_tagging(
            Bucket=bucket_name,
            Key= resumeFile,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'Expires',
                        'Value': 'True'
                    },
                ]
            }
        )
    except Exception as e:
        raise Exception('Error occurred while tagging resume for expiration: ', e)

def update_job_status(jobId, emailId):
    try:
        response = table.update_item(
            Key={
                'jobId': jobId,
                'emailId': emailId
            },
            UpdateExpression="set #status = :s",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':s': 'reject'
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        raise Exception('Error occurred while updating job status: ', e)
    
