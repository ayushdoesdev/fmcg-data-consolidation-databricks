# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC use catalog fmcg;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create schema
# MAGIC Silver and bronze schema are for child company (Sportbar) and Altikon is parent company
# MAGIC Gold schema will contain data from both parent and child company

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check how many records in fact tables
# MAGIC select count(*) from fmcg.gold.fact_orders;