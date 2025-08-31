import os
import json
import boto3
import psycopg2
from psycopg2.extras import execute_values

sqs = boto3.client('sqs')

DB_ENDPOINT = os.environ['DB_ENDPOINT']
DB_NAME = os.environ.get('DB_NAME', 'order_db')
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
SQS_URL = os.environ['SQS_URL']

# Connect to Aurora Postgres
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_ENDPOINT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432
    )
    return conn

def handler(event, context):
    # # Receive messages (batch size 1, but can be adjusted)
    # print('Receiving messages')
    # response = sqs.receive_message(
    #     QueueUrl=SQS_URL,
    #     MaxNumberOfMessages=1,
    #     WaitTimeSeconds=10
    # )
    # print('Received messages')

    records = event.get('Records', [])

    # messages = response.get('Messages', [])
    # if not messages:
    #     print("No messages in queue.")
    #     return

    if not records:
        print("No records in event.")
        return

    print('records:', records)

    print(f"Processing {len(records)} records")
    for msg in records:
        receipt_handle = msg['receiptHandle']
        body = json.loads(msg['body'])
        
    
        body['status'] = 'pending'
    
        order_id = msg['messageId']
        user_id = body['user_id']
        product_id = body['product_id']
        quantity = body.get('quantity', 1)
        order_date = body.get('order_date')
        status = body['status']

        # Insert into DB
        try:
            print('Inserting into DB')
            conn = get_db_connection()
            with conn:
                with conn.cursor() as cur:
                    insert_query = """
                        INSERT INTO orders (order_id,user_id, product_id, quantity, order_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING;
                    """
                    cur.execute(insert_query, (order_id,user_id, product_id, quantity, order_date, status))
                    print('Inserted record successfully')
            conn.close()
        except Exception as e:
            print(f"DB Insert error: {e}")
            return
        finally:
            print('Record processed')

        # Delete message from queue after successful insert
        sqs.delete_message(
            QueueUrl=SQS_URL,
            ReceiptHandle=receipt_handle
        )
        print(f"Processed and deleted message {receipt_handle}")
