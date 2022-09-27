-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Drop all tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/martvaformula1dl/presentation/"

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/martvaformula1dl/processed/"
