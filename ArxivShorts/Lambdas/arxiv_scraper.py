import json
import boto3
import os
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup, element
import logging

# ARXIV_PARSER

# =====================
# ENVIRONMENT VARIABLES
# =====================
base_url = os.getenv("BASE_URL")
queue_url = os.getenv("SQS_QUEUE_URL")

# =======================
# RESOURCE INITIALIZATION
# =======================
sqs_client = boto3.client('sqs')

TODAY_DATE = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

# =======================
# LOGGER CONFIGURATION
# =======================
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # default log level


# ============================
# EXTRACT HTML PAGE DOM
# ============================
def get_recent_html(date):
    try:
        url = base_url.replace("{{date}}", date)
        logger.debug("Fetching URL: %s", url)
        response = urllib.request.urlopen(url)
        data = response.read().decode('utf-8')
        return data
    except Exception as e:
        logger.error("Error fetching HTML: %s", e)
        return None


# ============================
# PARSE HTML CONTENT
# ============================
def parse_dom(html):
    """
    Parse Arxiv HTML page and return a list of articles.
    Only includes articles that have an HTML URL.
    JSON fields: batch_id, article_id, title, abstract, authors, url
    """
    articles = []
    total_articles = 0

    try:
        soup = BeautifulSoup(html, 'html.parser')
        articles_dl = soup.find('dl', id='articles')
        if not articles_dl:
            logger.warning("No <dl id='articles'> found in HTML")
            return articles

        current_paper_id = None
        html_url = None

        for child in articles_dl.children:
            if not isinstance(child, element.Tag):
                continue

            total_articles += 1

            if child.name == 'dt':
                current_paper_id = None
                html_url = None

                abs_link = child.find('a', href=lambda href: href and '/abs/' in href)
                if abs_link and 'id' in abs_link.attrs:
                    current_paper_id = abs_link['id'].strip()

                html_link = child.find('a', href=lambda href: href and '/html/' in href)
                if html_link and 'href' in html_link.attrs:
                    html_url = html_link['href'].strip()

            elif child.name == 'dd' and current_paper_id and html_url:
                title_div = child.find('div', class_='list-title mathjax')
                title = title_div.get_text(strip=True).replace('Title:', '').strip() if title_div else ''

                abstract_p = child.find('p', class_='mathjax')
                abstract = abstract_p.get_text(strip=True) if abstract_p else ''

                authors_div = child.find('div', class_='list-authors')
                authors = [a.get_text(strip=True) for a in authors_div.find_all('a')] if authors_div else []

                articles.append({
                    'batch_id': TODAY_DATE,
                    'article_id': current_paper_id,
                    'title': title,
                    'abstract': abstract,
                    'authors': authors,
                    'url': html_url
                })

                current_paper_id = None
                html_url = None

        logger.info("Processed a total of %d items. Found HTML links for %d items", total_articles, len(articles))
        return articles

    except Exception as e:
        logger.error("Error parsing DOM: %s", e)
        return []


# ============================
# LAMBDA HANDLER
# ============================
def lambda_handler(event, context):
    try:
        logger.info("Today's date: %s", TODAY_DATE)
        dom = get_recent_html(TODAY_DATE)
        articles = parse_dom(dom)

        if not articles:
            logger.warning("No articles found for date: %s", TODAY_DATE)
            return

        logger.info("Parsed %d articles for date: %s", len(articles), TODAY_DATE)

    except Exception as ex:
        logger.error("Error while fetching or parsing the DOM: %s", ex)
        return

    # send each article to SQS
    try:
        logger.info("Pushing up to 100 out of %d messages to SQS", len(articles))
        for article in articles[:100]:
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(article)
            )
        logger.info("Successfully pushed messages to SQS")

    except Exception as ex:
        logger.error("Error while sending messages to SQS: %s", ex)
        return
