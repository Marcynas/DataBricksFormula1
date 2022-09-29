# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### collecting and renaming data

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}circuits") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f"{processed_folder_path}constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","v_race_date")

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}results") \
.filter(f"file_date = '{v_file_date}'")
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id","result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### joining circuits to races

# COMMAND ----------

joined_races_circuits_df = races_df \
.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
.select(races_df.race_id,races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining results with other tables

# COMMAND ----------

semi_final_df = results_df \
.join(joined_races_circuits_df, results_df.result_race_id == joined_races_circuits_df.race_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = semi_final_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date") \
.withColumn("crated_date", current_timestamp()) \
.withColumn("result_file_date", "file_date")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}race_results")

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','race_results','race_id')
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_presentation','race_results', presentation_folder_path, merge_condition, 'race_id')
