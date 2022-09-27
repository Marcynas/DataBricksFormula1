-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Purple;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
as
Select team_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points,
       RANK() OVER(order by AVG(calculated_points) desc) team_rank
  from f1_presentation.calculated_race_results
  group by team_name
  having count(1) >= 100
  order by avg_points desc

-- COMMAND ----------

Select race_year,
       team_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank <= 5 )
  group by race_year, team_name
  order by race_year, avg_points desc

-- COMMAND ----------

Select race_year,
       team_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank <= 10 )
  group by race_year, team_name
  order by race_year, avg_points desc
