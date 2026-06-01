# Databricks notebook source
# MAGIC %md
# MAGIC ### Create schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze COMMENT 'Bronze layer'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Silver layer'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold COMMENT 'Gold layer'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download files to volume

# COMMAND ----------

# DBTITLE 1,Download individual parquet files
months = ["01", "02", "03", "04", "05"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Use Unity Catalog Volume - files will be visible in Data Explorer
landing_path = "/Volumes/workspace/default/nyc_taxi_data/landing/"
bronze_path = "/Volumes/workspace/default/nyc_taxi_data/bronze/taxi_trips"
silver_path = "/Volumes/workspace/default/nyc_taxi_data/silver/taxi_trips"

import requests
import os

# Create landing directory if it doesn't exist
os.makedirs(landing_path, exist_ok=True)

for m in months:
    file_name = f"yellow_tripdata_2023-{m}.parquet"
    url = BASE_URL + file_name

    local_path = os.path.join(landing_path, file_name)

    r = requests.get(url, stream=True)
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded {file_name}")

# COMMAND ----------

# MAGIC %sql select * from bronze.bronze_yellow_taxi where passenger_count is null

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check file structure and normalization for column names

# COMMAND ----------

# DBTITLE 1,Check schemas across all files
import os
from pyspark.sql import functions as F

landing_path = "/Volumes/workspace/default/nyc_taxi_data/landing/"
months = ["01", "02", "03", "04", "05"]

print("Schema comparison across all parquet files:\n")
print("=" * 80)

schemas = {}
for m in months:
    file_path = f"{landing_path}yellow_tripdata_2023-{m}.parquet"
    if os.path.exists(file_path):
        df = spark.read.parquet(file_path)
        schemas[m] = set(df.columns)
        print(f"\n2023-{m}: {len(df.columns)} columns")
        print(f"Columns: {', '.join(sorted(df.columns))}")

# Find column differences
print("\n" + "=" * 80)
print("\nColumn name differences:")
all_columns = set()
for cols in schemas.values():
    all_columns.update(cols)

for col in sorted(all_columns):
    months_with_col = [m for m, cols in schemas.items() if col in cols]
    if len(months_with_col) < len(months):
        print(f"  '{col}' - present in months: {months_with_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations
# MAGIC - Parquets have different schema naming and ordering, need normalization
# MAGIC - will use lowercase to normalize and first parquet as source for schema to load up columns in order

# COMMAND ----------

# DBTITLE 1,Get lowercase normalized schema
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

# DBTITLE 1,Union all files with normalized schema
from pyspark.sql import functions as F

landing_path = "/Volumes/workspace/default/nyc_taxi_data/landing/"
months = ["01", "02", "03", "04", "05"]

print("Reading and normalizing all parquet files...\n")

# Read all files and normalize column names to lowercase
dfs_normalized = []
for m in months:
    file_path = f"{landing_path}yellow_tripdata_2023-{m}.parquet"
    if os.path.exists(file_path):
        # Read the file with its natural schema
        df = spark.read.parquet(file_path)
        
        # Normalize all column names to lowercase
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        
        # Add metadata columns
        df = df.withColumn("month", F.lit(m))
        df = df.withColumn("source_file", F.lit(file_path))
        
        dfs_normalized.append(df)
        print(f"✓ 2023-{m}: {df.count():,} records, {len(df.columns)} columns")

print(f"\nTotal files read: {len(dfs_normalized)}")

# Union all dataframes with allowMissingColumns to handle schema evolution
print("\nUnioning all dataframes...")
df_union = dfs_normalized[0]
for df in dfs_normalized[1:]:
    df_union = df_union.unionByName(df, allowMissingColumns=True)

print(f"\n✓ Union complete!")
print(f"  Total records: {df_union.count():,}")
print(f"  Total columns: {len(df_union.columns)}")
print(f"\nFinal schema (all lowercase):")
df_union.printSchema()

# COMMAND ----------

# DBTITLE 1,Write to bronze.nyc_taxi_trips table
# Create bronze schema if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Write the unioned data to bronze.nyc_taxi_trips
df_union.write.format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("bronze.nyc_taxi_trips")

print("✓ Table 'bronze.nyc_taxi_trips' created successfully!")
print(f"Total records: {spark.table('bronze.nyc_taxi_trips').count():,}")
print(f"Change Data Feed: ENABLED")
spark.table('bronze.nyc_taxi_trips').printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creates three schemas

# COMMAND ----------

# DBTITLE 1,Create bronze, silver, gold schemas
# Create the three schemas for medallion architecture
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze COMMENT 'Bronze layer - raw ingested data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Silver layer - cleaned and validated data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold COMMENT 'Gold layer - aggregated business metrics'")

print("✓ Schemas created successfully!\n")

# Verify they exist
schemas = spark.sql("SHOW SCHEMAS IN workspace LIKE 'bronze|silver|gold'").toPandas()
print("Medallion architecture schemas:")
print(schemas)

# COMMAND ----------

# MAGIC %sql select * from gold.gold_yellow_taxi

# COMMAND ----------

# MAGIC %sql select * from gold.may_hourly_avg_passengers

# COMMAND ----------

# MAGIC %sql select * from gold.monthly_avg_revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.monthly_avg_revenue