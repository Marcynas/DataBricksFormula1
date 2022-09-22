# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest contructors.json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Read the JSON file using dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema) \
.json("dbfs:/mnt/martvaformula1dl/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - drop the url column

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - rename and add ingestion timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - write the data to parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/martvaformula1dl/processed/constructors")

# COMMAND ----------


