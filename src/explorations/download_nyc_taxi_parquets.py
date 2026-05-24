# Databricks notebook source
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