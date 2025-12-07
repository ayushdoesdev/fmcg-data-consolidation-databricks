# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportbar-dataengg-fmcg-project/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# print check data type
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Normalise `month` field

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------


# 1️. Parse `month` from multiple possible formats
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_bronze.withColumn(
    "month",
    F.coalesce(
        F.try_to_date(F.col("month"), "yyyy/MM/dd"),
        F.try_to_date(F.col("month"), "dd/MM/yyyy"),
        F.try_to_date(F.col("month"), "yyyy-MM-dd"),
        F.try_to_date(F.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

df_silver.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Handling `gross_price`

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# We are validating the gross_price column, converting only valid numeric values to double, fixing negative prices by making them positive, and replacing all non-numeric values with 0


df_silver = df_silver.withColumn(
    "gross_price",
    F.when(F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'), 
           F.when(F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double"))
            .otherwise(F.col("gross_price").cast("double")))
    .otherwise(0)
)

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# We enrich the silver dataset by performing an inner join with the products table to fetch the correct product_code for each product_id.

df_products = spark.table("fmcg.silver.products") 
df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")

df_joined.show(5)

# COMMAND ----------

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true")\
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# COMMAND ----------

# select only required columns
df_gold = df_silver.select("product_code", "month", "gross_price")
df_gold.show(5)

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
df_gold_price.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get the price for each product_code (aggregated by year)

# COMMAND ----------

df_gold_price = (
    df_gold_price
    .withColumn("year", F.year("month"))
    # 0 = non-zero price, 1 = zero price  ➜ non-zero comes first
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(F.col("is_zero"), F.col("month").desc())
)


df_gold_latest_price = (
    df_gold_price
      .withColumn("rnk", F.row_number().over(w))
      .filter(F.col("rnk") == 1)
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Excellent 👏 — this is a **very clean and common PySpark pattern** for finding the **latest valid (non-zero) record per product per year**.
# MAGIC
# MAGIC Let’s unpack it carefully, line by line, so you understand *exactly* what each part does and what the final output will look like.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 1. Base DataFrame: `df_gold_price`
# MAGIC
# MAGIC You start with something like this:
# MAGIC
# MAGIC | product_code | month      | gross_price |
# MAGIC | ------------ | ---------- | ----------- |
# MAGIC | P1           | 2024-01-01 | 100.0       |
# MAGIC | P1           | 2024-03-01 | 0.0         |
# MAGIC | P1           | 2024-06-01 | 120.0       |
# MAGIC | P2           | 2024-04-01 | 0.0         |
# MAGIC | P2           | 2024-09-01 | 150.0       |
# MAGIC | P2           | 2023-10-01 | 90.0        |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 2. Add Derived Columns
# MAGIC
# MAGIC ```python
# MAGIC df_gold_price = (
# MAGIC     df_gold_price
# MAGIC     .withColumn("year", F.year("month"))
# MAGIC     .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 💡 What happens:
# MAGIC
# MAGIC | product_code | month      | gross_price | year | is_zero |
# MAGIC | ------------ | ---------- | ----------- | ---- | ------- |
# MAGIC | P1           | 2024-01-01 | 100.0       | 2024 | 0       |
# MAGIC | P1           | 2024-03-01 | 0.0         | 2024 | 1       |
# MAGIC | P1           | 2024-06-01 | 120.0       | 2024 | 0       |
# MAGIC | P2           | 2024-04-01 | 0.0         | 2024 | 1       |
# MAGIC | P2           | 2024-09-01 | 150.0       | 2024 | 0       |
# MAGIC | P2           | 2023-10-01 | 90.0        | 2023 | 0       |
# MAGIC
# MAGIC Explanation:
# MAGIC
# MAGIC * `F.year("month")` extracts just the year from the date.
# MAGIC * `is_zero` is a flag:
# MAGIC
# MAGIC   * `1` means **gross_price == 0**
# MAGIC   * `0` means **gross_price > 0**
# MAGIC
# MAGIC 💡 The comment explains the logic:
# MAGIC
# MAGIC > “0 = non-zero price, 1 = zero price — so that **non-zero prices come first** in the sort order later.”
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 3. Define the Window
# MAGIC
# MAGIC ```python
# MAGIC w = (
# MAGIC     Window
# MAGIC     .partitionBy("product_code", "year")
# MAGIC     .orderBy(F.col("is_zero"), F.col("month").desc())
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This defines a **windowing logic**:
# MAGIC
# MAGIC * `partitionBy("product_code", "year")`:
# MAGIC   → Treat each product and each year separately.
# MAGIC * `orderBy(F.col("is_zero"), F.col("month").desc())`:
# MAGIC   → Within each group, sort first by:
# MAGIC
# MAGIC   1. `is_zero` ascending (non-zero first, since 0 < 1)
# MAGIC   2. `month` descending (latest date first)
# MAGIC
# MAGIC So you're ranking rows such that:
# MAGIC
# MAGIC * The **most recent non-zero price** appears first.
# MAGIC * If no non-zero price exists, the latest zero price will appear.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 4. Add a Row Number and Filter for Rank = 1
# MAGIC
# MAGIC ```python
# MAGIC df_gold_latest_price = (
# MAGIC     df_gold_price
# MAGIC       .withColumn("rnk", F.row_number().over(w))
# MAGIC       .filter(F.col("rnk") == 1)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This means:
# MAGIC
# MAGIC * Add a **rank number** within each `(product_code, year)` window based on the sort order we just defined.
# MAGIC * Keep only the **first row (rnk == 1)** — i.e. the latest valid record.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 5. ✅ Final Output
# MAGIC
# MAGIC | product_code | month      | gross_price | year | is_zero | rnk |
# MAGIC | ------------ | ---------- | ----------- | ---- | ------- | --- |
# MAGIC | P1           | 2024-06-01 | 120.0       | 2024 | 0       | 1   |
# MAGIC | P2           | 2024-09-01 | 150.0       | 2024 | 0       | 1   |
# MAGIC | P2           | 2023-10-01 | 90.0        | 2023 | 0       | 1   |
# MAGIC
# MAGIC 💡 Explanation:
# MAGIC
# MAGIC * For `P1`, year 2024 → non-zero prices are (100, 120), latest is `2024-06-01`.
# MAGIC * For `P2`, year 2024 → latest non-zero is `2024-09-01`.
# MAGIC * For `P2`, year 2023 → only one entry → selected.
# MAGIC
# MAGIC So you now have the **latest non-zero price per product per year**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Summary of Each Step
# MAGIC
# MAGIC | Step                                   | Purpose                          |
# MAGIC | -------------------------------------- | -------------------------------- |
# MAGIC | `withColumn("year", F.year("month"))`  | Extract year from date           |
# MAGIC | `withColumn("is_zero", ...)`           | Mark whether price is zero       |
# MAGIC | `Window.partitionBy(...).orderBy(...)` | Group and sort by year + product |
# MAGIC | `F.row_number().over(w)`               | Assign rank within each group    |
# MAGIC | `.filter(F.col("rnk") == 1)`           | Keep only latest non-zero price  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 TL;DR
# MAGIC
# MAGIC > This code finds the **most recent non-zero gold price per product per year**,
# MAGIC > preferring **non-zero prices** and picking the **latest month** within each year.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Would you like me to show a **diagram or visual timeline** showing how Spark orders and ranks the rows before filtering? That really helps visualize how the window and ranking work.
# MAGIC

# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

## Take required cols

df_gold_latest_price = df_gold_latest_price.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")

# change year to string
df_gold_latest_price = df_gold_latest_price.withColumn("year", F.col("year").cast("string"))

df_gold_latest_price.show(5)

# COMMAND ----------

df_gold_latest_price.printSchema()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")


delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()