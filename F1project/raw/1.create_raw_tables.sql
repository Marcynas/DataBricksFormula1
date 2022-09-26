-- Databricks notebook source
Create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create circuits table

-- COMMAND ----------

Drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
options (path "/mnt/martvaformula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races table

-- COMMAND ----------

Drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
options (path "/mnt/martvaformula1dl/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructors table

-- COMMAND ----------

Drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
options (path "/mnt/martvaformula1dl/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table

-- COMMAND ----------

Drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: string, surname: string>,
dob DATE,
nationality STRING,
url STRING
)
USING json
options (path "/mnt/martvaformula1dl/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create results table

-- COMMAND ----------

Drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId INT,
raceId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusID STRING
)
USING json
options (path "/mnt/martvaformula1dl/raw/results.json", header true)

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit stops table

-- COMMAND ----------

Drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING json
options (path "/mnt/martvaformula1dl/raw/pit_stops.json", multiLine true, header true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create lap times table

-- COMMAND ----------

Drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
options (path "/mnt/martvaformula1dl/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create qualifying table

-- COMMAND ----------

Drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId int,
raceId INT
)
USING json
options (path "/mnt/martvaformula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying
