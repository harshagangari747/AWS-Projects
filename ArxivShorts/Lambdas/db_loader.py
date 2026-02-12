import json
import boto3
import os
import logging

# DB_LOADER

# =====================
# ENVIRONMENT VARIABLES
# =====================
DYNAMO_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# =======================
# RESOURCE INITIALIZATION
# =======================
s3_client = boto3.client("s3", region_name="us-west-2")
dynamodb = boto3.resource("dynamodb", region_name="us-west-2")

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set INFO as default


# =====================
# READ FILE FROM S3
# =====================
def read_file_from_s3(s3_bucket, s3_key):
    logger.debug("Fetching S3 object s3://%s/%s", s3_bucket, s3_key)
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    content = response["Body"].read()
    return content


# ===================
# WRITE TO DYNAMODB
# ===================
def write_items_to_dynamo_batch(table_name, items):
    table = dynamodb.Table(table_name)

    logger.info("Writing items to DynamoDB table %s", table_name)
    
    with table.batch_writer() as batch:
        for item in items:
            # Skip dummy records
            if item.get("paper_id", "").startswith("dummy_"):
                logger.warning("Skipping dummy record: %s", item.get("paper_id"))
                continue

            # Validate required keys
            if not item.get("fetch_date") or not item.get("paper_id"):
                logger.warning("Skipping item with missing PK/SK: %s", item)
                continue

            try:
                batch.put_item(Item=item)
            except Exception as e:
                logger.error("Failed to write item %s: %s", item.get("paper_id", "UNKNOWN"), e)


# ======================
# PARSE S3 FILE CONTENT
# ======================
def parse_s3_file_content(content):
    items = []

    if isinstance(content, bytes):
        content = content.decode("utf-8")

    lines = content.strip().split("\n")

    for line in lines:
        try:
            record = json.loads(line)

            record_id = record.get("recordId", "")
            model_input_text = (
                record.get("modelInput", {})
                .get("messages", [{}])[0]
                .get("content", [{}])[0]
                .get("text", "")
            )

            if record_id.startswith("dummy_") or "SKIP PARSING THIS RECORD" in model_input_text:
                logger.info("Skipping dummy record: %s", record_id)
                continue

            if "#" not in record_id:
                logger.warning("Skipping invalid recordId: %s", record_id)
                continue

            batch_id, article_id = record_id.split("#", 1)

            model_output = record.get("modelOutput", {})
            text_content_list = model_output.get("content", [])

            text_str = ""
            if text_content_list and "text" in text_content_list[0]:
                text_str = text_content_list[0]["text"]

            try:
                parsed_text = json.loads(text_str)
            except json.JSONDecodeError:
                logger.warning("Failed to parse JSON inside model output for record %s", record_id)
                parsed_text = {}

            item = {
                "fetch_date": batch_id,
                "paper_id": article_id,
                "headline": parsed_text.get("headline", ""),
                "summary": parsed_text.get("summary", ""),
                "eyebrow": parsed_text.get("eyebrow", ""),
                "articleUrl": parsed_text.get("url", ""),
                "authors": parsed_text.get("authors", []),
                "articleId": parsed_text.get("articleId", article_id)
            }

            items.append(item)

        except Exception as e:
            logger.error("Skipping invalid line: %s", e)

    return items


# =====================
# LAMBDA HANDLER
# =====================
def lambda_handler(event, context):
    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket_name = s3_info.get("bucket", {}).get("name")
        object_key = s3_info.get("object", {}).get("key")

        if not object_key.endswith(".jsonl.out") or "inferred-outputs" not in object_key:
            logger.info("Skipping object %s", object_key)
            continue

        logger.info("Processing S3 object: s3://%s/%s", bucket_name, object_key)

        try:
            content = read_file_from_s3(bucket_name, object_key)
            items = parse_s3_file_content(content)

            logger.info("Parsed %d items from %s", len(items), object_key)

            if items:
                write_items_to_dynamo_batch(DYNAMO_TABLE_NAME, items)

            logger.info("Successfully wrote %d records to DynamoDB table %s", len(items), DYNAMO_TABLE_NAME)

        except Exception as e:
            logger.error("Error processing %s: %s", object_key, e)

    return {
        "statusCode": 200,
        "body": json.dumps("S3 file processed and loaded into DynamoDB")
    }
