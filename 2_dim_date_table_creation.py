# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

start_date = '2024-01-01'
end_date = '2025-12-01'

# COMMAND ----------

# Generate 1 row per month start between start_date and end_date
df = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'), 
                to_date('{end_date}'), 
                interval 1 month
            )
        ) AS month_start_date
    """)
)

# add useful analytics column
df = (
    df
    .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.lit(F.quarter("month_start_date"))))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
)

# COMMAND ----------

display(df)

# COMMAND ----------

# now save the dataframe as table

df.write \
    .mode("overwrite") \
    .format("delta") \
        .saveAsTable("fmcg.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Here is what is going on above
# MAGIC
# MAGIC Sure! 😊 Let’s go through this **PySpark** code step by step — in **simple terms** and with an **example** so it’s easy to understand.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 What this code does
# MAGIC
# MAGIC It **creates a DataFrame** with **one row per month** between two given dates (e.g., from `2023-01-01` to `2023-12-01`)
# MAGIC and then adds some **useful date-related columns** for analytics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Step-by-step explanation
# MAGIC
# MAGIC #### 1. Create monthly dates between `start_date` and `end_date`
# MAGIC
# MAGIC ```python
# MAGIC df = (
# MAGIC     spark.sql(f"""
# MAGIC         SELECT explode(
# MAGIC             sequence(
# MAGIC                 to_date('{start_date}'), 
# MAGIC                 to_date('{end_date}'), 
# MAGIC                 interval 1 month
# MAGIC             )
# MAGIC         ) AS month_start_date
# MAGIC     """)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Let’s assume:
# MAGIC
# MAGIC ```python
# MAGIC start_date = "2023-01-01"
# MAGIC end_date = "2023-06-01"
# MAGIC ```
# MAGIC
# MAGIC ##### 👉 What happens here:
# MAGIC
# MAGIC * `sequence(to_date('2023-01-01'), to_date('2023-06-01'), interval 1 month)`
# MAGIC   → creates a list of dates spaced by **1 month**:
# MAGIC
# MAGIC   ```
# MAGIC   [2023-01-01, 2023-02-01, 2023-03-01, 2023-04-01, 2023-05-01, 2023-06-01]
# MAGIC   ```
# MAGIC
# MAGIC * `explode(...)` turns that list into **multiple rows**, one per date.
# MAGIC
# MAGIC So the DataFrame (`df`) now looks like this:
# MAGIC
# MAGIC | month_start_date |
# MAGIC | ---------------- |
# MAGIC | 2023-01-01       |
# MAGIC | 2023-02-01       |
# MAGIC | 2023-03-01       |
# MAGIC | 2023-04-01       |
# MAGIC | 2023-05-01       |
# MAGIC | 2023-06-01       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Add extra columns for analytics
# MAGIC
# MAGIC ```python
# MAGIC df = (
# MAGIC     df
# MAGIC     .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
# MAGIC     .withColumn("year", F.year("month_start_date"))
# MAGIC     .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
# MAGIC     .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
# MAGIC     .withColumn("quarter", F.concat(F.lit("Q"), F.lit(F.quarter("month_start_date"))))
# MAGIC     .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Let’s break down each column:
# MAGIC
# MAGIC | Column               | Meaning                            | Example (for 2023-03-01) |
# MAGIC | -------------------- | ---------------------------------- | ------------------------ |
# MAGIC | **date_key**         | A numeric key (YYYYMM)             | `202303`                 |
# MAGIC | **year**             | Year number                        | `2023`                   |
# MAGIC | **month_name**       | Full month name                    | `March`                  |
# MAGIC | **month_short_name** | Short month name                   | `Mar`                    |
# MAGIC | **quarter**          | Quarter of the year (Q1, Q2, etc.) | `Q1`                     |
# MAGIC | **year_quarter**     | Year and quarter combined          | `2023-Q1`                |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧾 Final Output Example
# MAGIC
# MAGIC | month_start_date | date_key | year | month_name | month_short_name | quarter | year_quarter |
# MAGIC | ---------------- | -------- | ---- | ---------- | ---------------- | ------- | ------------ |
# MAGIC | 2023-01-01       | 202301   | 2023 | January    | Jan              | Q1      | 2023-Q1      |
# MAGIC | 2023-02-01       | 202302   | 2023 | February   | Feb              | Q1      | 2023-Q1      |
# MAGIC | 2023-03-01       | 202303   | 2023 | March      | Mar              | Q1      | 2023-Q1      |
# MAGIC | 2023-04-01       | 202304   | 2023 | April      | Apr              | Q2      | 2023-Q2      |
# MAGIC | 2023-05-01       | 202305   | 2023 | May        | May              | Q2      | 2023-Q2      |
# MAGIC | 2023-06-01       | 202306   | 2023 | June       | Jun              | Q2      | 2023-Q2      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Summary
# MAGIC
# MAGIC In plain English:
# MAGIC
# MAGIC > This code makes a list of all the first days of each month between two dates,
# MAGIC > then adds columns like month name, year, quarter, and a numeric key (YYYYMM)
# MAGIC > to help with reporting and analytics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Would you like me to show how to do the same thing in **pure Python (Pandas)** instead of Spark, for easier testing locally?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count('id') from fmcg.gold.dim_date