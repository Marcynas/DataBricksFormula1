# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying.json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Reading the JSON file using dataframe reader

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(),True),
    StructField("raceId", IntegerType(),True),
    StructField("driverId", IntegerType(),True),
    StructField("constructorId", IntegerType(),True),
    StructField("number", IntegerType(),True),
    StructField("position", IntegerType(),True),
    StructField("q1", StringType(),True),
    StructField("q2", StringType(),True),
    StructField("q3", StringType(),True),
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine",True) \
.json("/mnt/martvaformula1dl/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Renaming columns and adding new ones

# COMMAND ----------

qualifying_final_df = qualifying_df \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - write the data to parquet

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/martvaformula1dl/processed/qualifying")
