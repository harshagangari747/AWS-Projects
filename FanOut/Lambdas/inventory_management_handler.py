import json
import os
import boto3
from decimal import Decimal

dynamodb_table = os.environ['INVENTORY_TABLE_NAME']
table = boto3.resource('dynamodb').Table(dynamodb_table)

def lambda_handler(event, context):
    try:
        # Parse SNS message
        record = event['Records'][0]
        message = json.loads(record['Sns']['Message'])

        print("Received message:", message)

        items = message['items']
        for item in items:
            product_id = item['productId']
            quantity = item['quantity']

            # Update inventory: Decrement currentStock
            update_inventory(product_id, quantity)

        print("Inventory updated successfully.")

    except Exception as e:
        print("Error updating inventory:", e)

def update_inventory(product_id, quantity):
    try:
        response = table.update_item(
            Key={'productId': product_id},
            UpdateExpression="SET currentStock = currentStock - :qty",
            ConditionExpression="currentStock >= :qty",
            ExpressionAttributeValues={
                ':qty': Decimal(quantity)
            },
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated {product_id}: New stock = {response['Attributes']['currentStock']}")
    except Exception as e:
            print(f"Error updating inventory for product {product_id}: {e}")
            raise