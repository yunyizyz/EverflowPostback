import functions_framework
import requests
import json
import base64
import logging
from google.cloud import pubsub_v1, secretmanager
from cloudevents.http import CloudEvent

logging.basicConfig(level=logging.INFO)

POSTBACK_URL_TEMPLATE = 'https://www.cznfe8trk.com/?nid=3026&oid={offer_id}&affid={affiliate_id}&amount={sale_amount}'
SECRET_ID = 'projects/cf-prod-main-share-110124/secrets/everflow-api-key/versions/latest'

secret_client = secretmanager.SecretManagerServiceClient()

def fetch_secret(secret_id):
    response = secret_client.access_secret_version(request={"name": secret_id})
    return response.payload.data.decode("UTF-8")


def send_postback(offer_id, affiliate_id, sale_amount):
    API_KEY = fetch_secret(SECRET_ID)
    url = POSTBACK_URL_TEMPLATE.format(offer_id=offer_id, affiliate_id=affiliate_id, sale_amount=sale_amount)
    headers = {
        'X-Eflow-API-Key': API_KEY, 
        'Content-Type': 'application/json'
    }
    logging.info(f"Sending data: oid:{offer_id}&affid={affiliate_id}&amount={sale_amount}")
    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        logging.info(f"HTTP Status Code: {response.status_code}")  
        return '', 200
    else:
        logging.error(f"HTTP error occurred: Status Code {response.status_code}, Response: {response.text}")
        return '', 500

# Trigger
@functions_framework.cloud_event
def pubsub_listener(event: CloudEvent):

    if event.data:     
        message_data = event.data['message']['data']
        decoded_message = base64.b64decode(message_data).decode('utf-8')
        logging.info(f"Received message: {decoded_message}")

        data = json.loads(decoded_message)

        if 'offer_id' in data and 'affiliate_id' in data and isinstance(data.get('sale_cnt'), (int, float)):
            send_postback(data['offer_id'], data['affiliate_id'], data['sale_cnt'])
        else:
            logging.info(f"Invalid data: offer_id={data.get('offer_id')}, affiliate_id={data.get('affiliate_id')}, sale_cnt={data.get('sale_cnt')}")
            return '', 500
