# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Gold Layer

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS zomato_sales.gold")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Sales Summary

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table("zomato_sales.silver.zomato_sales")

df_daily_sales = (
    df_silver
    .groupBy("order_day")
    .agg(
        F.sum(F.col("quantity") * F.col("price")).alias("total_revenue"),
        F.sum("quantity").alias("total_items_sold"),
        F.countDistinct("order_id").alias("total_orders")
    )
    .orderBy("order_day")
)

df_daily_sales.write.mode("overwrite").saveAsTable("zomato_sales.gold.daily_sales")

display(df_daily_sales)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales By Store

# COMMAND ----------

df_sales_by_store = (
    df_silver
    .groupBy("store_id")
    .agg(
        F.sum(F.col("quantity") * F.col("price")).alias("store_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.countDistinct("order_id").alias("total_orders")
    )
    .orderBy(F.desc("store_revenue"))
)

df_sales_by_store.write.mode("overwrite").saveAsTable("zomato_sales.gold.sales_by_store")

display(df_sales_by_store)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Products

# COMMAND ----------

df_top_products = (
    df_silver
    .groupBy("cleaned_product_name")
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum(F.col("quantity") * F.col("price")).alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)

df_top_products.write.mode("overwrite").saveAsTable("zomato_sales.gold.top_products")

display(df_top_products)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Revenue Table

# COMMAND ----------

df_fact_revenue = (
    df_silver
    .withColumn("revenue", F.col("quantity") * F.col("price"))
    .select(
        "order_id",
        "product_id",
        "cleaned_product_name",
        "quantity",
        "price",
        "revenue",
        "store_id",
        "order_timestamp",
        "order_day"
    )
)

df_fact_revenue.write.mode("overwrite").saveAsTable("zomato_sales.gold.fact_revenue")

display(df_fact_revenue)
