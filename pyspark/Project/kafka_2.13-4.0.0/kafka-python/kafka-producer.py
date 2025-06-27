import os
import time
import json
import datetime
import pandas as pd
from kafka import KafkaProducer

# Parameters
topic = 'crime-chicago'
parquet_file = '../../Datasets/crimes-small-test'
sleeptime = 1.0  # message interval in seconds

# Date conversion for JSON
def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()

# Callback on successful dispatch
def on_send_success(metadata):
    print(f'Published to {metadata.topic} partition {metadata.partition} offset {metadata.offset}')

# Callback on error
def on_send_error(excp):
    print('Error while sending:', excp)

# Connecting to Kafka
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(3, 9),
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Reading parquet
df = pd.read_parquet(parquet_file)

print(f'Total rows to send: {len(df)}')

# Sending to Kafka line by line
for index, row in df.iterrows():
    data = row.to_dict()
    json_data = json.dumps(data, default=datetime_converter)

    print(f'-- Sending: {json_data}')

    try:
        kafka_producer.send(topic, value=json_data)\
            .add_callback(on_send_success)\
            .add_errback(on_send_error)
    except Exception as e:
        print(f'Error: {e}')

    time.sleep(sleeptime)

# Closing the connection
kafka_producer.close()
