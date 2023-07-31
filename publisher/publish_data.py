from datetime import datetime
from google.cloud import pubsub_v1
import json
import os

project_id = os.environ['GOOGLE_CLOUD_PROJECT']
topic_id = os.environ['GCP_PUBSUB_TOPIC']

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

id = "1"
table = "transactions"
db = "visibility"
now = datetime.now()
ts = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
ts_ms = int(datetime.timestamp(now)*1000)
payload_data = {
    "transactionId": id,
    "sellerId": "cee9170b-3040-40fe-a774-3c283764e63d",
    "sellerName": "John Doe",
    "sellerEmail": "john.doe@gmail.com",
    "buyerId": "1",
    "buyerName": "Jane Doe",
    "buyerEmail": "jane.doe@gmail.com",
    "productId": "1", 
    "totalAmount": 10.0, 
    "createdAt": "2023-07-04T16:00:00.000Z", 
    "modifiedAt": "2023-07-04T16:00:00.000Z"
}

data = {
    "id": id,
    "table": table,
    "db": db,
    "op": "c",
    "data": json.dumps(payload_data),
    "ts": ts,
    "ts_ms": ts_ms,
    "metadata": {
        "version": "1.0"
    }
}
future = publisher.publish(topic_path, json.dumps(data).encode())
future.result()
print(f"Message published!")
