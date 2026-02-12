import json
import os
import boto3
import logging
from boto3.dynamodb.conditions import Key

# REQUEST_HANDLER

# =====================
# ENVIRONMENT VARIABLES
# =====================
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
PAGE_SIZE = 10

# ========================
# LOGGER CONFIGURATION
# ========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ========================
# RESOURCE INITIALIZATIONS
# ========================
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE_NAME)
logger.info("DynamoDB table initialized: %s", DYNAMODB_TABLE_NAME)


def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", event)

        # Get query parameters
        query_params = event.get("queryStringParameters") or {}
        fetch_date = query_params.get("date")
        page = int(query_params.get("page", 1))

        if not fetch_date:
            logger.warning("Missing required query parameter: date")
            return response(400, {"message": "Missing required query parameter: date"})

        if page < 1:
            logger.warning("Invalid page number: %d", page)
            return response(400, {"message": "Page number must be >= 1"})

        logger.info("Fetching page %d for date %s", page, fetch_date)

        # We simulate pagination by iterating pages
        exclusive_start_key = None
        current_page = 1

        while current_page <= page:
            query_kwargs = {
                "KeyConditionExpression": Key("fetch_date").eq(fetch_date),
                "Limit": PAGE_SIZE
            }

            if exclusive_start_key:
                query_kwargs["ExclusiveStartKey"] = exclusive_start_key

            result = table.query(**query_kwargs)
            logger.debug("Query result: %s", result)

            if current_page == page:
                items = result.get("Items", [])
                last_evaluated_key = result.get("LastEvaluatedKey")

                logger.info(
                    "Returning page %d with %d articles, has_more=%s",
                    page, len(items), last_evaluated_key is not None
                )

                return response(200, {
                    "date": fetch_date,
                    "page": page,
                    "page_size": PAGE_SIZE,
                    "has_more": last_evaluated_key is not None,
                    "articles": items
                })

            exclusive_start_key = result.get("LastEvaluatedKey")

            if not exclusive_start_key:
                logger.info("Requested page %d exceeds available data", page)
                return response(200, {
                    "date": fetch_date,
                    "page": page,
                    "page_size": PAGE_SIZE,
                    "has_more": False,
                    "articles": []
                })

            current_page += 1

    except Exception as e:
        logger.error("Error in lambda_handler: %s", e, exc_info=True)
        return response(500, {"message": str(e)})


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
