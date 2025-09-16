import json
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError
import os

from typing import Any

# === CONFIGURATION ===
TABLE_NAME = "SampleTable"
JSON_FILE = "sample.json"

# === INIT DYNAMODB ===

profile = os.environ.get('AWS_PROFILE')
orders_table_name = os.environ.get('ORDERS_TABLE_NAME')
AWS_REGION = os.environ.get('AWS_REGION')


session = boto3.Session(profile_name=profile)
dynamodb = boto3.resource('dynamodb',region_name=AWS_REGION)
table = dynamodb.Table(orders_table_name)

# === RECURSIVELY CONVERT FLOATS TO DECIMALS ===
def convert_floats(obj: Any) -> Any:
    if isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def upload_items(json_file):
    try:
        with open(json_file) as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("JSON file must contain a list of items")

        # Convert float -> Decimal
        items = convert_floats(data)

        for item in items:
            print(f"Inserting item: {item}")
            table.put_item(Item=item)

        print("✅ Upload complete!")

    except FileNotFoundError:
        print(f"❌ File not found: {json_file}")
    except ClientError as e:
        print(f"❌ AWS error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    upload_items('sample_orders.json')
