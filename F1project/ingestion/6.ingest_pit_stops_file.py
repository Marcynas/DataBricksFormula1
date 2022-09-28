# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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
.json(f"{raw_folder_path}{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Renaming columns and adding new ones

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 3 - write the data to parquet~~

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

overwrite_partition(pit_stops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

dbutils.notebook.exit("Done 😎")
