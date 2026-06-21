# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType,DateType
from pyspark.sql.functions import lit,col,when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # The Transfomration Logic

# COMMAND ----------

df = (
    spark.table("workspace.silver.crm_products").alias("pn")
    .join(spark.table("workspace.silver.erp_product_category").alias("pc"),col("pn.category_id") == col("pc.category_id"),how = "left")
    .select(
        F.row_number().over(Window.orderBy("pn.start_date","pn.product_number")).alias("product_key"),
        "pn.product_id",
        "pn.product_number",
        "pn.product_name",
        "pn.category_id",
        "pc.category",
        "pc.subcategory",
        "pc.maintenace_flag",
        "pn.product_line",
        "pn.start_date"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sanity the check of gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  workspace.gold.dim_products