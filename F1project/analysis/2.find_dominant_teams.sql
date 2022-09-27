-- Databricks notebook source
Select team_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  group by team_name
  having count(1) >= 100
  order by avg_points desc

-- COMMAND ----------


