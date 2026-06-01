# Databricks notebook source
# MAGIC %md
# MAGIC ### Observations
# MAGIC - Parquets have different schema naming and ordering, need normalization
# MAGIC - will use lowercase to normalize and first parquet as source for schema to load up columns in order

# COMMAND ----------

from pyspark.sql.types import StructType, StructField

# Read the source schema
source_df = spark.read.parquet("/Volumes/workspace/default/nyc_taxi_data/landing/yellow_tripdata_2023-01.parquet")

# Create lowercase normalized schema
lowercase_fields = []
for field in source_df.schema.fields:
    lowercase_fields.append(StructField(field.name.lower(), field.dataType, field.nullable))

lowercase_schema = StructType(lowercase_fields)

print("Original schema:")
print(source_df.schema)
print("\n" + "="*80 + "\n")
print("Lowercase normalized schema:")
print(lowercase_schema)
print("\n" + "="*80 + "\n")
print("Lowercase column names:")
print([field.name for field in lowercase_schema.fields])

# COMMAND ----------

# MAGIC %sql select * from bronze.bronze_yellow_taxi where vendorid is null

# COMMAND ----------

# MAGIC %sql select VendorID, count(1) from parquet.`/Volumes/workspace/default/nyc_taxi_data/landing/yellow_tripdata_2023-01.parquet`  group by all
# MAGIC -- where VendorID is null

# COMMAND ----------

# MAGIC %sql select * from parquet.`/Volumes/workspace/default/nyc_taxi_data/landing/yellow_tripdata_2023-01.parquet` where passenger_count is null

# COMMAND ----------

# MAGIC %sql select * from bronze.bronze_yellow_taxi where passenger_count is null

# COMMAND ----------

# MAGIC %sql select VendorID, count(1) from parquet.`/Volumes/workspace/default/nyc_taxi_data/landing/yellow_tripdata_2023-02.parquet`  group by all
# MAGIC -- where VendorID is null

# COMMAND ----------

# MAGIC %sql select * from silver.silver_yellow_taxi where vendorid is null

# COMMAND ----------

# MAGIC %sql select * from gold.gold_yellow_taxi

# COMMAND ----------

# MAGIC %sql select * from gold.may_hourly_avg_passengers

# COMMAND ----------

# MAGIC %sql select * from gold.monthly_avg_revenue