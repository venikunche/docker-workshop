#Q1
```
docker run -it --rm --entrypoint=bash python:3.13
pip -V
```
Answer: pip 25.3

#Q3
```
SELECT count(*)
FROM green_taxi_data
WHERE (lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01')
AND trip_distance <= 1
```

#Q4
```
SELECT *
FROM green_taxi_data
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1
```

#Q5
```
SELECT z."Zone", SUM(t.total_amount) AS total_amount_sum
FROM green_taxi_data t
JOIN taxi_zone_data z ON t."PULocationID" = z."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) = CAST('2025-11-18' AS DATE)
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1
```

#Q6
```
SELECT lpep_pickup_datetime, pz."Zone" AS pickup_zone, dz."Zone" AS dropoff_zone,
    t.tip_amount
FROM green_taxi_data t
JOIN taxi_zone_data pz ON t."PULocationID" = pz."LocationID"
JOIN taxi_zone_data dz ON t."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
AND (lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01')
ORDER BY t.tip_amount DESC
LIMIT 1
```