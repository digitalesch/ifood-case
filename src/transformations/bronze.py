from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, LongType

@dp.materialized_view(
    name="workspace.bronze.bronze_yellow_taxi",
    comment="NYC yellow taxi trip data loaded from parquet files",
    table_properties={
        "delta.enableChangeDataFeed": "true",  # Enable CDF for downstream tracking
        "delta.feature.timestampNtz": "supported",  # Enable timestamp without timezone
        "quality": "bronze"
    }
)
def bronze_yellow_taxi():
    """
    Reads individual parquet files, casts to consistent schema, then unions them.
    This handles type mismatches across different months.
    """
    landing_path = "/Volumes/workspace/default/nyc_taxi_data/landing/"
    
    # List of files to process
    files = [
        "yellow_tripdata_2023-01.parquet",
        "yellow_tripdata_2023-02.parquet",
        "yellow_tripdata_2023-03.parquet",
        "yellow_tripdata_2023-04.parquet",
        "yellow_tripdata_2023-05.parquet"
    ]
    
    # Read and normalize each file
    dfs = []
    for file in files:
        df = spark.read.format("parquet").load(f"{landing_path}{file}")
        
        # Cast all columns to consistent types BEFORE renaming
        df = df.withColumn("VendorID", col("VendorID").cast(LongType()))
        df = df.withColumn("passenger_count", col("passenger_count").cast(DoubleType()))
        df = df.withColumn("RatecodeID", col("RatecodeID").cast(DoubleType()))
        df = df.withColumn("PULocationID", col("PULocationID").cast(LongType()))
        df = df.withColumn("DOLocationID", col("DOLocationID").cast(LongType()))
        df = df.withColumn("Airport_fee", col("Airport_fee").cast(DoubleType()))
        
        # Normalize column names to lowercase
        for column_name in df.columns:
            df = df.withColumnRenamed(column_name, column_name.lower())
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        df = df.withColumn("source_file", lit(f"{landing_path}{file}"))
        
        dfs.append(df)
    
    # Union all dataframes
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)
    
    return result
