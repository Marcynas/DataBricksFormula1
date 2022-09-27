-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Purple;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
Select driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points,
       RANK() OVER(order by AVG(calculated_points) desc) driver_rank
  from f1_presentation.calculated_race_results
  group by driver_name
  having count(1) >= 50
  order by avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10 )
  group by race_year, driver_name
  order by race_year, avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10 )
  group by race_year, driver_name
  order by race_year, avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10 )
  group by race_year, driver_name
  order by race_year, avg_points desc

-- COMMAND ----------


