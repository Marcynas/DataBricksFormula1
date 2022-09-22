# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times.csv file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Reading the JSON file using dataframe reader

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(),False),
    StructField("driverId", IntegerType(),True),
    StructField("lap", IntegerType(),True),
    StructField("position", IntegerType(),True),
    StructField("time", StringType(),True),
    StructField("milliseconds", IntegerType(),True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema) \
.csv("/mnt/martvaformula1dl/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Renaming columns and adding new ones

# COMMAND ----------

lap_times_final_df = lap_times_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - write the data to parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/martvaformula1dl/processed/lap_times")