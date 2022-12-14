# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.json(f"{raw_folder_path}{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - rename and add new columns

# COMMAND ----------

results_final_df = results_df \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_Order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.withColumn("ingestion_date", current_timestamp()) \
.drop(col("statusId"))

# COMMAND ----------

results_deduped_df =results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ~~step 3 - write the data to parquet~~

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 - Writing data to f1_proccesed database as parquet

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Done ????")
