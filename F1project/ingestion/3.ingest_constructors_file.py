# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest contructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 - Read the JSON file using dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema) \
.json(f"{raw_folder_path}constructors.json")

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
.withColumn("data_source", lit(v_data_source)) \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 4 - write the data to parquet~~

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Done 😎")
