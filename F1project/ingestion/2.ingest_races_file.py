# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, to_timestamp, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Creating schema and reading races.csv file

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", StringType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema) \
.csv(f"{raw_folder_path}races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Renaming columns
# MAGIC Can be done in step 5 using .alias()

# COMMAND ----------

races_renamed_df = races_df \
.withColumnRenamed("raceID", ("race_id")) \
.withColumnRenamed("year", ("race_year")) \
.withColumnRenamed("circuitId", ("circuit_id")) \
.withColumnRenamed("lng", ("longitude")) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - Adding ingestion timestamp and race_timestamp

# COMMAND ----------

races_withTimestamp_df = races_renamed_df \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - Sellecting important columns

# COMMAND ----------

races_final_df  = races_withTimestamp_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 5 - Writing data to datalake as parquet (partitioned)~~

# COMMAND ----------

# races_final_df.write.mode("overwrite").partitionBy('name').parquet(f"{processed_folder_path}races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 5 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Done 😎")
