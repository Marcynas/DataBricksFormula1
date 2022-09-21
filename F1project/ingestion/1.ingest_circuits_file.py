# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Imports

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Creating schema and reading circuits.csv file

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema) \
.csv("dbfs:/mnt/martvaformula1dl/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - Sellecting important columns

# COMMAND ----------

circuits_selected_df  = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - Renaming columns
# MAGIC Can be done in step 3 using .alias()

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
.withColumnRenamed("circuitId", ("circuit_id")) \
.withColumnRenamed("circuitRed", ("circuit_ref")) \
.withColumnRenamed("lat", ("latitude")) \
.withColumnRenamed("lng", ("longitude")) \
.withColumnRenamed("alt", ("altitude")) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 5 - Adding ingestion timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 6 - Writing data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/martvaformula1dl/processed/circuits")
