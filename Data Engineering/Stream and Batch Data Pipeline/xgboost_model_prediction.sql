WITH 
main_table AS (
SELECT
  CAST(VendorID AS STRING) AS vendor_id,
  TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) AS duration,
  CAST(passenger_count AS INT) AS passenger_count,
  CAST(trip_distance AS NUMERIC) AS trip_distance,
  CAST(RatecodeID AS STRING) AS rate_code,
  store_and_fwd_flag,
  CAST(payment_type AS STRING) AS payment_type,
  CAST(fare_amount AS NUMERIC) AS fare_amount,
  CAST(extra AS NUMERIC) AS extra,
  CAST(mta_tax AS NUMERIC) AS mta_tax,
  CAST(tip_amount AS NUMERIC) AS tip_amount,
  CAST(tolls_amount AS NUMERIC) AS tolls_amount,
  CAST(improvement_surcharge	AS NUMERIC) AS improvement_surcharge,
  CAST(total_amount AS NUMERIC) AS total_amount,
  CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,
  CAST(airport_fee AS NUMERIC) AS airport_fee,
  CAST(PULocationID AS STRING) AS pickup_location_id,
  CAST(DOLocationID AS STRING) AS dropoff_location_id,
  data_date,
  uid
FROM
  `latihan-345909.nyc_taxi.yellow_taxi_data_stream`
  WHERE trip_distance != 0 
    AND fare_amount !=0
    AND trip_distance IS NOT NULL
    AND fare_amount IS NOT NULL
)

SELECT
  *
FROM
  ML.PREDICT(MODEL `latihan-345909.nyc_taxi.nyc_taxi_duration_model`,
    (
    SELECT
      *
    FROM
      main_table
    )
  )

  