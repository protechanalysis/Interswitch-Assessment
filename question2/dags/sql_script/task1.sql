WITH weekend_trips AS (
  SELECT 
    formatDateTime(pickup_date, '%Y-%m-%d') AS trip_month,
    dayOfWeek(pickup_date) AS day_of_week,
    fare_amount,
    pickup_datetime,
    dropoff_datetime
  FROM tripdata
  WHERE pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
    AND dayOfWeek(pickup_date) IN (6, 7) 
),
duration as (
select trip_month, 
    day_of_week, fare_amount, 
    dateDiff('minute', pickup_datetime, dropoff_datetime) as trip_duration
from weekend_trips
),
trip_aggregation as (
select trip_month, day_of_week, count(*) as total_trip, avg(fare_amount) as average_fare, avg(trip_duration) as average_trip_duration
from duration
group by trip_month, day_of_week
),
avg_sat as (
select substring(trip_month, 1, 7) AS trip_month_sat,
    day_of_week, avg(total_trip) as sat_mean_trip_count, avg(average_fare) as sat_mean_fare_per_trip, avg(average_trip_duration) as sat_mean_duration_per_trip
from trip_aggregation 
where day_of_week = 6
group by trip_month_sat, day_of_week
),
avg_sun as (
select substring(trip_month, 1, 7) AS trip_month_sun,
    avg(total_trip) as sun_mean_trip_count, avg(average_fare) as sun_mean_fare_per_trip, avg(average_trip_duration) as sun_mean_duration_per_trip
from trip_aggregation 
where day_of_week = 7
group by trip_month_sun
),
combine_table as (
select trip_month_sun as months, sat_mean_trip_count, sat_mean_fare_per_trip, sat_mean_duration_per_trip, sun_mean_trip_count, sun_mean_fare_per_trip, sun_mean_duration_per_trip
from avg_sat sat
join avg_sun sun
on sat.trip_month_sat = sun.trip_month_sun
)

select *
from combine_table
order by months
