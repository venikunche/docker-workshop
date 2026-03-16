import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models_green import green_trip_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    value_deserializer=green_trip_deserializer,
    consumer_timeout_ms=10000
)

print(f"Listening to {topic_name}...")

count = 0
over_5 = 0

for message in consumer:
    trip = message.value
    count += 1
    if trip.trip_distance > 5.0:
        over_5 += 1
    if count % 10000 == 0:
        print(f"Processed {count} messages so far, {over_5} with distance > 5")

print(f"\nTotal messages: {count}")
print(f"Trips with trip_distance > 5: {over_5}")

consumer.close()
