import json
import os
import uuid
import boto3
import urllib.request
import datetime
import logging
from bs4 import BeautifulSoup, element

# PROCESSOR


# =========================
# ENVIRONMENT VARIABLES
# =========================
PROMPT = os.getenv("PROMPT")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")

# =========================
# LOGGER CONFIGURATION
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Default log level

logger.info("PROMPT set as: %s", PROMPT)

# =========================
# AWS Clients
# =========================
s3_resource = boto3.resource("s3", region_name="us-west-2")
bucket = s3_resource.Bucket(BUCKET_NAME)

sqs_client = boto3.client("sqs", region_name="us-west-2")
dynamodb = boto3.client("dynamodb")

# =========================
# Helpers
# =========================
def get_html_dom(url: str) -> str | None:
    """Fetch article HTML"""
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.read().decode("utf-8")
    except Exception as e:
        logger.warning("Error fetching HTML for %s: %s", url, e)
        return None

#=================
# EXTRACT SECTIONS
#=================
def extract_sections(html: str) -> dict:
    """
    Extract Introduction, Experiment/Methods, Results sections
    from arXiv HTML using TOC
    """
    extracted = {
        "introduction": "",
        "experiment": "",
        "results": ""
    }

    try:
        soup = BeautifulSoup(html, "html.parser")

        nav = soup.find("nav", class_="ltx_TOC")
        if not nav:
            return extracted

        toc = nav.find("ol", class_="ltx_toclist")
        if not toc:
            return extracted

        for li in toc.find_all("li"):
            a_tag = li.find("a", href=True)
            if not a_tag:
                continue

            section_title = a_tag.get_text(strip=True).lower()
            href = a_tag["href"]

            if "#" not in href:
                continue

            section_id = href.split("#", 1)[1]
            section_tag = soup.find("section", id=section_id)
            if not section_tag:
                continue

            text = section_tag.get_text(separator=" ", strip=True)

            if "introduction" in section_title:
                extracted["introduction"] = text
            elif "experiment" in section_title or "method" in section_title:
                extracted["experiment"] += ("\n\n" if extracted["experiment"] else "") + text
            elif "result" in section_title or "evaluation" in section_title:
                extracted["results"] += ("\n\n" if extracted["results"] else "") + text

        return extracted

    except Exception as e:
        logger.error("Error extracting sections: %s", e)
        return extracted

#=================
# UPDATE DYNAMODB
#=================
def update_batch_statistics(batch_id: str, success: int, failure: int):
    """Atomic batch update in DynamoDB"""
    try:
        dynamodb.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={"batch_id": {"S": batch_id}},
            UpdateExpression="ADD success_count :s, failure_count :f",
            ExpressionAttributeValues={
                ":s": {"N": str(success)},
                ":f": {"N": str(failure)}
            }
        )
        logger.info("Updated DynamoDB for batch %s: success=%d, failure=%d", batch_id, success, failure)
    except Exception as e:
        logger.error("Error updating DynamoDB for batch %s: %s", batch_id, e)


# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):
    records = event.get("Records", [])
    logger.info("Received event with %d records", len(records))

    prefix = None
    batch = []
    success_count = 0
    failure_count = 0

    try:
        batch_id = json.loads(records[0]["body"])["batch_id"]
        logger.info("========> Process started for batch %s with %d records", batch_id, len(records))
    except Exception as e:
        logger.error("Failed to extract batch_id from event: %s", e)
        return {"statusCode": 400, "body": "Invalid event format"}

    for record in records:
        receipt_handle = record["receiptHandle"]

        try:
            message = json.loads(record["body"])
            paper_id = message["article_id"]
            url = message["url"]
            batch_id = message["batch_id"]
            if not prefix:
                prefix = batch_id

            logger.info("Processing paper %s", paper_id)

            html = get_html_dom(url)
            if not html:
                failure_count += 1
                raise Exception("HTML fetch failed")

            # ---- extract sections safely ----
            sections = extract_sections(html)
            if not any(sections.values()):
                logger.warning("No usable sections for paper %s", paper_id)
                failure_count += 1
                continue

            authors_str = ", ".join(message.get("authors", []))

            llm_record = {
                "recordId": '#'.join([batch_id, paper_id]),
                "modelInput": {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 350,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        f"{PROMPT}\n\n"
                                        f"Title:\n{message['title']}\n\n"
                                        f"ArticleId:\n{message['article_id']}\n\n"
                                        f"Abstract:\n{message['abstract']}\n\n"
                                        f"Introduction:\n{sections['introduction']}\n\n"
                                        f"Experiment:\n{sections['experiment']}\n\n"
                                        f"Results:\n{sections['results']}\n\n"
                                        f"Authors:\n{authors_str}\n\n"
                                        f"Article URL:\n{message['url']}"
                                    )
                                }
                            ]
                        }
                    ]
                }
            }

            batch.append(llm_record)
            success_count += 1

        except Exception as e:
            logger.error("Processing error for record: %s", e)

        finally:
            # Always delete SQS message to avoid poison loops
            try:
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )
            except Exception as e:
                logger.warning("Failed to delete SQS message: %s", e)

    # ---- Save batch to S3 ----
    if batch:
        key = f"{prefix}/input_jsons/{uuid.uuid4()}.json"
        bucket.put_object(Key=key, Body=json.dumps(batch))
        logger.info("Saved batch to s3://%s/%s", BUCKET_NAME, key)

    # ---- Update DynamoDB once ----
    update_batch_statistics(batch_id, success_count, failure_count)

    return {
        "statusCode": 200,
        "success": success_count,
        "failure": failure_count
    }
