# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, FloatType

catalog_name ='zomato_sales'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Bronze table

# COMMAND ----------

catalog_name = "zomato_sales"

df_bronze = spark.table(f"{catalog_name}.bronze.zomato_sales")
display(df_bronze)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean & transform

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("store_id", F.col("store_id").cast("int"))
    .withColumn("order_timestamp", F.to_timestamp("order_date"))
    .withColumn("order_day", F.to_date("order_date"))
    .withColumn("cleaned_product_name", F.coalesce(F.trim("product_name"), F.lit("unknown")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicates

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["order_id", "product_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sliver Layer 

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS zomato_sales.silver")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Ready Data

# COMMAND ----------

df_silver.write.mode("overwrite").saveAsTable("zomato_sales.silver.zomato_sales")
