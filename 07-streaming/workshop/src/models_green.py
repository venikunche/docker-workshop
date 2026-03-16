import json
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


def green_trip_deserializer(data):
    json_str = data.decode('utf-8')
    trip_dict = json.loads(json_str)
    return GreenTrip(**trip_dict)
