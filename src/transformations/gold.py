from pyspark import pipelines as dp
from pyspark.sql.functions import sum, count, avg, min, max

@dp.materialized_view(
    name="workspace.gold.gold_yellow_taxi",
    comment="Table for consumption",
    cluster_by=["vendorid"],
    table_properties={
        "delta.feature.timestampNtz": "supported"  # Enable timestamp without timezone support
    }
)
def gold_yellow_taxi():
    # Read from silver as batch (not streaming) for aggregation
    df = spark.read.table("workspace.silver.silver_yellow_taxi")
    
    # Aggregate by year and month
    gold = (
        df
            .groupBy(
                "vendorid",
                "pickup_year", 
                "pickup_month"
            )
        .agg(
            count("*").alias("total_trips"),
            sum("passenger_count").alias("total_passengers"),
            sum("total_amount").alias("total_revenue"),
            min("tpep_pickup_datetime").alias("first_trip_time"),
            max("tpep_pickup_datetime").alias("last_trip_time")
        )
    )
    
    return gold
