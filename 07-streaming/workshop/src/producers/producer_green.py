import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from dataclasses import dataclass


@dataclass
class GreenTrip:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float


def green_trip_from_row(row):
    return GreenTrip(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']) if pd.notna(row['passenger_count']) else 0.0,
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def green_trip_serializer(trip):
    trip_dict = dataclasses.asdict(trip)
    json_str = json.dumps(trip_dict)
    return json_str.encode('utf-8')


# Read green taxi data for October 2025
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]
df = pd.read_parquet(url, columns=columns)

print(f'Total rows: {len(df)}')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=green_trip_serializer
)

topic_name = 'green-trips'

t0 = time.time()

for idx, row in df.iterrows():
    trip = green_trip_from_row(row)
    producer.send(topic_name, value=trip)
    if idx % 5000 == 0:
        print(f"Sent: {idx} rows")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
