import json
import boto3
import uuid
import os
import logging

# INFERENCE_HANDLER
# =====================
# ENVIRONMENT VARIABLES
# =====================
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
BEDROCK_ROLE_ARN = os.getenv("BEDROCK_ROLE_ARN")
MODEL_ID = os.getenv("MODEL_ID")

# =======================
# RESOURCE INITIALIZATION
# =======================
s3_resource = boto3.resource('s3', region_name='us-west-2')
bedrock_client = boto3.client('bedrock', region_name='us-west-2')

# =======================
# LOGGER CONFIGURATION
# =======================
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Default log level


# =======================
# PARSE DYNAMODB RECORD
# =======================
def parse_dynamodb_record(record):
    """
    Extract batch_id, success_count, failure_count, and compute total_count
    from a DynamoDB MODIFY stream record.
    """
    if record["eventName"] != "MODIFY":
        logger.info("Skipping non-MODIFY event: %s", record["eventName"])
        return None  # Skip non-MODIFY events

    dynamodb = record["dynamodb"]
    new_image = dynamodb["NewImage"]
    keys = dynamodb["Keys"]

    batch_id = keys["batch_id"]["S"]
    success_count = int(new_image["success_count"]["N"])
    failure_count = int(new_image["failure_count"]["N"])
    total_count = success_count + failure_count

    logger.info("Parsed DynamoDB record: batch_id=%s, total_count=%d", batch_id, total_count)
    return {
        "batch_id": batch_id,
        "success_count": success_count,
        "failure_count": failure_count,
        "total_count": total_count
    }


# =======================
# COMPILE JSON FROM S3
# =======================
def compile_json_from_s3(bucket_name, batch_id):
    compiled_data = []
    prefix = f"{batch_id}/input_jsons"

    logger.info("Collecting JSON files from bucket=%s, prefix=%s", bucket_name, prefix)
    bucket = s3_resource.Bucket(bucket_name)
    response = bucket.objects.filter(Prefix=prefix)
    logger.debug("S3 response iterator: %s", response)

    for obj in response:
        key = obj.key
        logger.debug("Parsing object key: %s", key)
        if key.endswith(".json"):
            logger.info("Reading file: %s", key)
            content = obj.get()["Body"].read().decode("utf-8")
            try:
                json_data = json.loads(content)
                if isinstance(json_data, list):
                    compiled_data.extend(json_data)
                else:
                    compiled_data.append(json_data)
            except json.JSONDecodeError:
                logger.warning("Skipping invalid JSON file: %s", key)

    logger.info("Total compiled records: %d", len(compiled_data))

    # Padding additional records to make 100
    if len(compiled_data) < 100:
        dummy_count = 100 - len(compiled_data)
        logger.info("Padding %d additional dummy records to make 100", dummy_count)
        for i in range(dummy_count):
            paper_id = f"dummy_{i+1}"
            dummy_record = {
                "recordId": paper_id,
                "modelInput": {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 10,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        "SKIP PARSING THIS RECORD\n\n"
                                        "Title:\n\n"
                                        "ArticleId:\n\n"
                                        "Abstract:\n\n"
                                        "Introduction:\n\n"
                                        "Experiment:\n\n"
                                        "Results:\n\n"
                                        "Authors:\n\n"
                                        "Article URL:\n"
                                    )
                                }
                            ]
                        }
                    ]
                }
            }
            compiled_data.append(dummy_record)

    logger.info("Compiled data length after padding: %d", len(compiled_data))
    return compiled_data


# =======================
# WRITE JSONL TO S3
# =======================
def write_jsonl_to_s3(compiled_data, batch_id):
    logger.info("Saving compiled data as .jsonl file")
    s3_key = f"{batch_id}/output_jsonl/batch_prompts.jsonl"

    jsonl_lines = [json.dumps(item) for item in compiled_data]
    jsonl_content = "\n".join(jsonl_lines)

    s3_object = s3_resource.Object(S3_BUCKET_NAME, s3_key)
    s3_object.put(
        Body=jsonl_content.encode("utf-8"),
        ContentType="application/json"
    )

    logger.info("Uploaded JSONL file to s3://%s/%s", S3_BUCKET_NAME, s3_key)
    return s3_key


# =======================
# START BEDROCK BATCH INFERENCE
# =======================
def start_bedrock_batch_inference(s3_bucket, s3_key, batch_id):
    logger.info("Starting Bedrock batch inference")

    input_s3_uri = f"s3://{s3_bucket}/{s3_key}"
    logger.debug("Input S3 URI: %s", input_s3_uri)

    output_s3_uri = f"s3://{s3_bucket}/inferred-outputs/{batch_id}/"
    logger.debug("Output S3 URI: %s", output_s3_uri)

    input_data_config = {"s3InputDataConfig": {"s3Uri": input_s3_uri}}
    output_data_config = {"s3OutputDataConfig": {"s3Uri": output_s3_uri}}

    response = bedrock_client.create_model_invocation_job(
        jobName=f"batch-inference-{batch_id}-{uuid.uuid4()}",
        modelId=MODEL_ID,
        roleArn=BEDROCK_ROLE_ARN,
        inputDataConfig=input_data_config,
        outputDataConfig=output_data_config
    )
    logger.info("Started Bedrock batch inference job for batch_id=%s", batch_id)


# =======================
# LAMBDA HANDLER
# =======================
def lambda_handler(event, context):
    try:
        record = event["Records"][0]
        event_name = record["eventName"]

        result = parse_dynamodb_record(record)
        if result is None:
            raise Exception("Event type is not MODIFY, skipping processing")

        total_count = result["total_count"]
        batch_id = result["batch_id"]

        logger.info("Total count from DynamoDB record: %d", total_count)

        if total_count >= 100:
            logger.info("Processing batch_id=%s from S3", batch_id)
            compiled_data = compile_json_from_s3(S3_BUCKET_NAME, batch_id)

            jsonl_s3_key = write_jsonl_to_s3(compiled_data, batch_id)
            start_bedrock_batch_inference(S3_BUCKET_NAME, jsonl_s3_key, batch_id)

    except Exception as ex:
        logger.error("Error while inferring batch: %s", ex)

    return {
        "statusCode": 200,
        "body": json.dumps("Hello from Lambda!")
    }
