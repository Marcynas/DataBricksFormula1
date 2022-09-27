-- Databricks notebook source
Select driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  group by driver_name
  having count(1) >= 50
  order by avg_points desc

-- COMMAND ----------


