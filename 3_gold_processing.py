# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Gold Processing

# COMMAND ----------

catalog = 'fmcg'
silver_schema = 'silver'
data_source = 'customers'
df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source};")


# take required columns only
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform","channel")


# COMMAND ----------

df_gold.show(truncate=False)

# COMMAND ----------

gold_schema = "gold"
df_gold.write \
    .mode("overwrite") \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# merge child table according to parent table
from delta.tables import DeltaTable
from pyspark.sql import functions as F

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")

df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Operation

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

