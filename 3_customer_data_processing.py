# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

print(bronze_schema)

# COMMAND ----------

# set the widgets
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, ",", data_source)

# COMMAND ----------

# specify the bucket
base_path = f"s3://sportbar-dataengg-fmcg-project/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

# read with defined header
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")      # This tells Spark to automatically detect column types.
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

"""
.select("*", "_metadata.file_name", "_metadata.file_size")

This line:
    Keeps all the original columns (*)
    Adds two extra metadata columns:
        _metadata.file_name: the name of the file each row came from
        _metadata.file_size: the size of that file (in bytes)
"""


display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze table
# MAGIC
# MAGIC
# MAGIC 🔹 Delta = a special format built on top of Parquet, with transaction logs and version control.
# MAGIC It allows features like:
# MAGIC   ACID transactions (safe writes)
# MAGIC   Time travel (query older versions)
# MAGIC   Incremental updates (Change Data Feed)
# MAGIC
# MAGIC 🔹 enableChangeDataFeed will turn on Change Data Feed for the table
# MAGIC This enables Change Data Feed (CDF) — a feature that tracks row-level changes (like inserts, updates, deletes) in Delta tables.
# MAGIC It makes your table capable of tracking data changes automatically.
# MAGIC %sql -> SELECT * FROM table_changes('your_table', 2)
# MAGIC to see what changed in version 2 of the table.
# MAGIC

# COMMAND ----------

df.write \
  .format("delta") \
  .option("delta.enableChangeDataFeed", "true") \
  .mode("overwrite") \
  .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
display(df_bronze)


# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# transformations

# duplicates
df_duplicates_count = df_bronze.groupBy("customer_id").count()
df_duplicates = df_duplicates_count.filter(F.col("count") > 1)
# df_duplicates = df_duplicates_count.where("count > 1")
display(df_duplicates)

# COMMAND ----------

print("Before duplicates: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print("After duplicates: ", df_silver.count())

# COMMAND ----------

# trim spaces
# check those values
display(
    df_silver.filter(
        F.col("customer_name") != F.trim(F.col("customer_name"))
    )
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)
display(df_silver)

# COMMAND ----------

# distinct cities
df_silver.select("city").distinct().show()

# COMMAND ----------

# fixing typos
city_mapping = {
    "Bengaluruu": "Bengaluru",
    "Bengalore": "Bengaluru",
    "Hyderabadd": "Hyderabad",
    "Hyderbad": "Hyderabad",
    "NewDelhee": "New Delhi",
    "NewDelhi": "New Delhi",
    "NewDheli": "New Delhi"
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
            .when(F.col("city").isin(allowed), F.col("city"))
            .otherwise(F.lit(None))
    )
)

df_silver.select("city").distinct().show()

# COMMAND ----------

# fix customer name cases
# df_silver.select("customer_name").distinct().show()

df_silver = df_silver.withColumn(
        "customer_name",
        F.when(F.col("customer_name").isNull(), None)
        .otherwise(F.initcap("customer_name"))
    )

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# which citys are null
df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

for x in df_silver.select("customer_name").filter(F.col("city").isNull()).collect():
    print(f"\"{x["customer_name"]}\"", end=", ")

# COMMAND ----------

null_customer_names = ["Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)


# COMMAND ----------

customer_city_fix = {
    789403: "New Delhi",
    789420: "Bengaluru",
    789521: "Hyderabad",
    789603: "Hyderabad"
}

# create a dataframe
df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)
display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")    # replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

df_silver.where("customer_id == 789403 or customer_id == 789521").show(truncate=False)

# COMMAND ----------

# convert customer_id to string
df_silver = df_silver.withColumn(
    "customer_id",
    F.col("customer_id").cast("string")
)
print(df_silver.printSchema())

# COMMAND ----------

# convert the columns according to parent table -> fmcg.gold.dim_customers

df_silver = (
    df_silver
    # Build final customer column: CustomerName-City or CustomerName-Unknown
    .withColumn(
        "customer",
        F.concat_ws("-", F.col("customer_name"), F.coalesce(F.col("city"), F.lit("unknown")))
    )

    # Static attributes aligned with Parent Data Model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquistion"))
)

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")