# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Reading the JSON file using dataframe reader

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(),False),
    StructField("raceId", IntegerType(),True),
    StructField("driverId", IntegerType(),True),
    StructField("constructorId", IntegerType(),True),
    StructField("number", IntegerType(),True),
    StructField("grid", IntegerType(),True),
    StructField("position", IntegerType(),True),
    StructField("positionText", StringType(),True),
    StructField("positionOrder", IntegerType(),True),
    StructField("points", FloatType(),True),
    StructField("laps", IntegerType(),True),
    StructField("time", StringType(),True),
    StructField("milliseconds", IntegerType(),True),
    StructField("fastestLap", IntegerType(),True),
    StructField("rank", IntegerType(),True),
    StructField("fastestLapTime", StringType(),True),
    StructField("fastestLapSpeed", StringType(),True),
    StructField("statusId", IntegerType(),True),
])

# COMMAND ----------

results_df = spark.read.schema(results_schema) \
.json(f"{raw_folder_path}results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - rename and add new columns

# COMMAND ----------

results_final_df = results_df \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructodId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_Order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("ingestion_date", current_timestamp()) \
.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - write the data to parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}results")
