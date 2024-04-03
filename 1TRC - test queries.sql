-- Databricks notebook source
use catalog amirskiy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1 Billion Row Challenge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Non-partitioned table

-- COMMAND ----------

SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Partitioned table

-- COMMAND ----------

SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1 Trillion Row Challenge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Non-partitioned table

-- COMMAND ----------

SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## Partitioned table

-- COMMAND ----------

SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;
