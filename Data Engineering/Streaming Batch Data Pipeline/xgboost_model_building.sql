#standardSQL
CREATE TEMP TABLE main_table AS
SELECT
  vendor_id,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS duration,
  passenger_count,
  trip_distance,
  rate_code,
  store_and_fwd_flag,
  payment_type,
  fare_amount,
  pickup_location_id,
  tolls_amount,
  airport_fee,
  dropoff_location_id
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
  WHERE trip_distance != 0 
    AND fare_amount !=0
    AND trip_distance IS NOT NULL
    AND fare_amount IS NOT NULL
    AND DATE(pickup_datetime) BETWEEN "2022-06-01" AND "2022-12-31";
 

CREATE OR REPLACE MODEL `latihan-345909.nyc_taxi.nyc_taxi_duration_model`
OPTIONS
  (model_type='BOOSTED_TREE_REGRESSOR',
   input_label_cols=['duration']) AS
SELECT *
FROM main_table