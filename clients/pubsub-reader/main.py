import sys
import base64
import datetime
import logging

from google.cloud import bigquery

client = bigquery.Client()
dataset_ref = client.dataset('stargaze')
table = client.get_table(client.dataset('stargaze').table('sensor'))

def main(event, context):
    # message
    # "{
    #     '@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 
    #     'attributes': {
    #         'deviceId': 'sensor', 
    #         'deviceNumId': '2667186399411002', 
    #         'deviceRegistryId': 'raspberry', 
    #         'deviceRegistryLocation': 'us-central1', 
    #         'gatewayId': 'default', 
    #         'projectId': 'danarchy-io', 
    #         'subFolder': ''
    #     }, 
    #     'data': 'MjAyMS0wNS0zMSAwMDo1NjoyMy45NzQ3ODIrMDA6MDAsMC4wMCwwLjAwLDEsMQ=='
    # }"
    # # context.event_id, context.timestamp, context.resource["name"]

    if 'data' in event and 'attributes' in event:
         if 'deviceId' in event['attributes']:
             if event['attributes']['deviceId'] == 'sensor':
                data = base64.b64decode(event['data']).decode('utf-8')
                date, h, t, flag_h, flag_t = data.split(',')
                date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f%z')
                date = int(date.timestamp())
                rows = [(date, h, t, flag_h, flag_t)]
                errors = client.insert_rows(table, rows)
                logging.error('rows = %s. error = %s', rows, errors)
                assert errors == []