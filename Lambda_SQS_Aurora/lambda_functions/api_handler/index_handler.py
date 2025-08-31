import json, boto3, os
from datetime import datetime
sqs = boto3.client('sqs')
QUEUE_URL = os.environ['SQS_URL']

REQUIRED_FIELDS = ["user_id", "product_id", "quantity", "order_date"]

def handler(event, context):
    print('event',event.get("body", "{}"))
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON"})

  
    missing_fields = [f for f in REQUIRED_FIELDS if f not in body]
    if missing_fields:
        return _response(400, {"error": f"Missing fields: {', '.join(missing_fields)}"})

   
    if not isinstance(body["user_id"], int):
        return _response(400, {"error": "user_id must be an integer"})
    if not isinstance(body["product_id"], int):
        return _response(400, {"error": "product_id must be an integer"})
    if not isinstance(body["quantity"], int) or body["quantity"] <= 0:
        return _response(400, {"error": "quantity must be a positive integer"})

    
    if not _is_valid_date(body["order_date"]):
        return _response(400, {"error": "order_date must be ISO 8601 format (e.g. 2025-08-02T12:00:00Z)"})

   
    try:
        print("Sending message to SQS",QUEUE_URL)
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(body)
        )
    except Exception as e:
        return _response(500, {"error": f"Failed to enqueue order: {str(e)}"})

    return _response(200, {"message": "Order queued successfully"})

def _response(status_code, body):
    """Return API Gateway-compatible response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

def _is_valid_date(date_string: str) -> bool:
    """Check if date is ISO 8601-ish."""
    try:
        datetime.strptime(date_string,"%Y-%m-%d")
        return True
    except ValueError:
        return False

