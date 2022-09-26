-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/martvaformula1dl/processed/"

-- COMMAND ----------

desc DATABASE f1_processed
