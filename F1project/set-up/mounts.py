# Databricks notebook source
# MAGIC %run "./functions" 

# COMMAND ----------

# MAGIC %run "./constants" 

# COMMAND ----------

unmount_adls(storage_account_name, containers)


# COMMAND ----------

mount_adls(storage_account_name, containers, secret)
