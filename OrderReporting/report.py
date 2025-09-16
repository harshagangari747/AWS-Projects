import json
from collections import defaultdict
from generate_pdf import make_pdf
from datetime import datetime, timedelta, timezone
import os
import boto3
from boto3.session import Session
from boto3.dynamodb.conditions import Key, Attr

orders_table_name = os.environ.get('ORDERS_TABLE_NAME')
products_table_name = os.environ.get('PRODUCTS_TABLE_NAME')
AWS_REGION = os.environ.get('AWS_REGION')

dynamodb_client = boto3.resource('dynamodb', region_name=AWS_REGION)

orders_table = dynamodb_client.Table(orders_table_name)
products_table = dynamodb_client.Table(products_table_name)


def process():

    now = datetime.now(timezone.utc)
    past_day = now - timedelta(hours=24)
    past_day = past_day.replace(hour=0, minute=0, second=0,microsecond=0)
    print('past day',past_day)

    orders = get_past_day_orders( past_day)



    total_sales = 0
    products_count = defaultdict(int)
    transaction_type = defaultdict(list)
    transaction_type["cc"] = [0,0]
    transaction_type["db"] = [0,0]
    transaction_type["gc"] = [0,0]

    report_data = defaultdict(object)
    product_wise_sales = defaultdict(float)

    #report data
    report_data['date'] = past_day
    report_data['total_orders'] = len(orders) if orders else 0
    report_data['total_sales'] = total_sales
    report_data['product_counts'] = products_count
    report_data['transaction_types'] = transaction_type
    report_data['product_wise_sales'] = product_wise_sales

    if not orders:
        print('orders fetched are None')
        make_pdf(report_data)
        return
        
    products = get_products(orders)

    for order in orders:
        total_sales += order['order_value']
        for product in order['order_items']:
            product_id = product['productId']
            products_count[product_id] += product['count']
        
        mode = order['transaction_type']
        transaction_type[mode][0] += 1
        transaction_type[mode][1] += order['order_value']
    
    
    price_map = {int(product['productId']):product['price'] for product in products}


    for product_id in products_count:
        product_count = products_count[product_id]

        product_wise_sales[product_id] = product_count * price_map[int(product_id)]

    report_data['date'] = past_day
    report_data['total_orders'] = len(orders)
    report_data['total_sales'] = total_sales
    report_data['product_counts'] = products_count
    report_data['transaction_types'] = transaction_type
    report_data['product_wise_sales'] = product_wise_sales


    print('report data', report_data)


    make_pdf(report_data)

    

def get_past_day_orders(past_date):

    isodate = past_date.isoformat()

    try:
        orders = orders_table.scan(
            FilterExpression=Attr('date').gte(isodate)
            )
        
        return orders['Items']
        
    except Exception as e:
        print('Exception occured while getting orders', e)
        return []


    
def get_products(orders):
    print('Querying products db', products_table_name)
    try:

        product_ids = [item['productId'] for order in orders for item in order['order_items']]
        product_id_set = set(product_ids)

        keys = [{'productId': pid} for pid in product_id_set]

        response = dynamodb_client.batch_get_item(
            RequestItems={
                products_table_name:{
                    'Keys': keys
                }
            }

        )    
        products = response['Responses']
        return products.get(products_table_name,{})
    
    except Exception as e:
        print('Error while getting products info', e)
        return {}

if __name__ == '__main__':
    process()



