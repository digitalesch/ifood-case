from pyspark import pipelines as dp
from pyspark.sql.functions import col, month, year

@dp.materialized_view(
    name="workspace.silver.silver_yellow_taxi",
    comment="Cleaned and validated NYC yellow taxi trip data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.feature.timestampNtz": "supported",  # Enable timestamp without timezone
        "quality": "silver"
    }
)
def silver_yellow_taxi():
    """
    Reads from bronze materialized view, applies data quality filters.
    All columns are in lowercase from bronze layer.
    
    Filters:
    - Valid trip metrics (distance, fare, total > 0)
    - passenger_count must be present (type cast fixed in bronze)
    - Only 2023 data, months 1-5 (Jan-May)
    """
    # Read from bronze as batch
    df = spark.read.table("workspace.bronze.bronze_yellow_taxi")
    
    # Add derived columns first
    df = df.withColumn("pickup_year", year("tpep_pickup_datetime"))
    df = df.withColumn("pickup_month", month("tpep_pickup_datetime"))
    
    # Apply data quality filters
    silver = (
        df
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("total_amount") > 0)
        .filter(col("passenger_count").isNotNull())  # Now that type casting is fixed, this should work
        .filter(col("pickup_year") == 2023)  # Only 2023 data
        .filter(col("pickup_month").between(1, 5))  # Only Jan-May
    )
    
    return silver
