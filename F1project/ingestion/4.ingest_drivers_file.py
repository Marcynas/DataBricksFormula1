# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Read the JSON file using dataframe reader

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(),True),
    StructField("surname", StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(),False),
    StructField("driverRef", StringType(),True),
    StructField("number", IntegerType(),True),
    StructField("code", StringType(),True),
    StructField("name", name_schema,True),
    StructField("dob", DateType(),True),
    StructField("nationality", StringType(),True),
    StructField("url", StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema) \
.json(f"{raw_folder_path}{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - rename and add new columns

# COMMAND ----------

drivers_renamed_df = drivers_df \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - drop unwanted

# COMMAND ----------

drivers_final_df = drivers_renamed_df \
.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 4 - write the data to parquet~~

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Done ????")

# COMMAND ----------


