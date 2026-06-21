# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType,IntegerType,StructField,StructType,DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import col,lit,when

# COMMAND ----------

# MAGIC %md
# MAGIC # The Transformation Table

# COMMAND ----------

df = (
    spark.table("workspace.silver.crm_sales").alias("sd")
    .join(spark.table("workspace.gold.dim_products").alias("pr"),col("sd.product_number") == col("pr.product_number"),how = "left")
    .join(spark.table("workspace.gold.dim_customer").alias("cu"),col("sd.customer_id") == col("cu.customer_id"),how = "left")
    .select(
        "sd.order_number",
        "pr.product_key",
        "cu.customer_id",
        "sd.order_date",
        "sd.ship_date",
        "sd.due_date",
        "sd.sales_amount",
        "sd.quantity",
        "sd.price")
    )

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # The Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sanity check of gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.fact_sales