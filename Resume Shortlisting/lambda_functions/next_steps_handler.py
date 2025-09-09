import json
import boto3
import os
import smtplib
from boto3.dynamodb.conditions import Key, Attr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

dynamodb = boto3.resource('dynamodb')
table_name = os.getenv('DYNAMODB_TABLE_NAME')
table = dynamodb.Table(table_name)


from_email_address = os.environ['FROM_EMAIL_ADDRESS']
app_password = os.environ['EMAIL_PASSWORD']

def lambda_handler(event, context):
    jobId = event['jobId']
    emailId = event['emailId']

    print('triggered next steps', event)

    # update the item in dynamodb with 'status'attribute set to 'pass'
    try:
        # update update the item in dynamodb with 'status'attribute set to 'pass'
        update_job_status(jobId, emailId)

        # send emailemail to the candidate
        send_email(jobId, emailId)

    except Exception as e:
        print('Error updating the state',e)
    finally:
        print('Candidate shortlisting completed!')
        
def send_email(jobId, receiver_email):
    sender_email = from_email_address
    print('sending email to', receiver_email)

    subject = f"Update on you recent application! Job ID: {jobId}"
    body = (
        f" <p> Congratulations! <br/> We are excited to inform you that you have been shortlisted for the Software Engineer position</p>"
        f"<p> at ABC Company. <br/> To move forward in the hiring process, you need to complete the following assessment with in 5 days</p>"
        f"<br/> Please find the link: <a href=''> Technical Assessment</a> <br/>"
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
    <p>All the best<br>ABC Company</p>
    </body>
    </html>"""
    
    message.attach(MIMEText(html_content, "html"))
    

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, message.as_string())

        print("Email sent successfully!")
        
    except Exception as e:
        raise Exception('Error occurred while sending email: ', e)


def update_job_status(jobId, emailId):
    try:
        table.update_item(
            Key={
                'jobId': jobId,
                'emailId': emailId
            },
            UpdateExpression="set #status = :s",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':s': 'pass'
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        raise Exception('Error occurred while updating job status: ', e)
    


