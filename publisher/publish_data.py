from datetime import datetime
from google.cloud import pubsub_v1
import json
import os

project_id = os.environ['GOOGLE_CLOUD_PROJECT']
topic_id = os.environ['GCP_PUBSUB_TOPIC']

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

id = "1"
# table = "users"
table = "transactions"
db = "visibility"
now = datetime.now()
ts = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
ts_ms = int(datetime.timestamp(now)*1000)
# payload_data = {
#     "userId": id, 
#     "username": "jane.doe", 
#     "email": "jane.doe@gmail.com", 
#     "password": "jane456", 
#     "loginCount": 2, 
#     "isActive": True, 
#     "createdAt": "2023-07-04T15:00:00.000Z", 
#     "modifiedAt": "2023-07-04T15:00:00.000Z"
# }
payload_data = {
    "userId": "1", 
    "transactionId": id, 
    "totalAmount": 10, 
    "email": "jane.doe@gmail.com", 
    "createdAt": "2023-07-04T15:00:00.000Z", 
    "modifiedAt": "2023-07-04T15:00:00.000Z"
}

data = {
    "id": id,
    "table": table,
    "db": db,
    "op": "ud",
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
