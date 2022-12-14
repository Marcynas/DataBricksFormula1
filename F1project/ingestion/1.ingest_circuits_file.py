# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Creating schema and reading circuits.csv file

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
.csv(f"{raw_folder_path}{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Sellecting important columns

# COMMAND ----------

circuits_selected_df  = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - Renaming columns
# MAGIC Can be done in step 3 using .alias()

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
.withColumnRenamed("circuitId", ("circuit_id")) \
.withColumnRenamed("circuitRed", ("circuit_ref")) \
.withColumnRenamed("lat", ("latitude")) \
.withColumnRenamed("lng", ("longitude")) \
.withColumnRenamed("alt", ("altitude")) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - Adding ingestion timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 5 - Writing data to datalake as parquet~~

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 5 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Done ????")
