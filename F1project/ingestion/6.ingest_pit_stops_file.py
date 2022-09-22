# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Reading the JSON file using dataframe reader

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(),False),
    StructField("driverId", IntegerType(),True),
    StructField("stop", StringType(),True),
    StructField("lap", IntegerType(),True),
    StructField("time", StringType(),True),
    StructField("duration", StringType(),True),
    StructField("milliseconds", IntegerType(),True),
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema) \
.option("multiline", True) \
.json("/mnt/martvaformula1dl/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Renaming columns and adding new ones

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - write the data to parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/martvaformula1dl/processed/pit_stops")
