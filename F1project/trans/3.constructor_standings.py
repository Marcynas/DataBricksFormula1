# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, count, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}race_results")

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}constructor_standings")
