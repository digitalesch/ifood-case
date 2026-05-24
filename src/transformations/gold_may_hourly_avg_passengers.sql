-- Question 3: Average passenger_count per hour of day in May across all taxis
CREATE VIEW workspace.gold.may_hourly_avg_passengers
COMMENT 'Average passenger count per hour of day for May trips across all yellow taxis'
AS
SELECT 
    HOUR(tpep_pickup_datetime) as pickup_hour,
    AVG(passenger_count) as avg_passengers_per_hour,
    COUNT(*) as total_trips_in_hour,
    SUM(passenger_count) as total_passengers
FROM workspace.silver.silver_yellow_taxi
WHERE pickup_month = 5
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY pickup_hour;
