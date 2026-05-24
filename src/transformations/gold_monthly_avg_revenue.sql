-- Question 2: Average total_amount received per month across all yellow taxis
CREATE VIEW workspace.gold.monthly_avg_revenue
COMMENT 'Average total amount received per month across all yellow taxis'
AS
SELECT 
    pickup_year,
    pickup_month,
    AVG(total_amount) as avg_total_amount_per_month,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue
FROM workspace.silver.silver_yellow_taxi
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;
