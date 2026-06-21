# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DateType,StringType
from pyspark.sql.functions import col,trim,length,when

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trimming

# COMMAND ----------

for field in df.schema.fields:
  if isinstance(field.dataType,StringType):
    df = df.withColumn(field.name,trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Table

# COMMAND ----------

df = (
    df
      .withColumn(
          "sls_order_dt",
          when((col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8),None)
          .otherwise(F.date_format(F.to_date(col("sls_order_dt").cast(StringType()),"yyyMMdd"),"dd-MM-yyyy"))
        )
    
    .withColumn(
        "sls_ship_dt",
        when((col("sls_ship_dt") == 0) | (length(col("sls_ship_dt"))  != 8),None)
        .otherwise(F.date_format(F.to_date(col("sls_ship_dt").cast(StringType()),"yyyyMMdd"),"dd-MM-yyyy"))
        )
    
    .withColumn(
        "sls_due_dt",
        when((col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8),None)
        .otherwise(F.date_format(F.to_date(col("sls_due_dt").cast(StringType()),"yyyyMMdd"),"dd-MM-yyyy"))
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales and Price Corrections

# COMMAND ----------

df = df.withColumn("sls_price",
                   when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                        when(col("sls_quantity") != 0,
                             col("sls_sales") / col("sls_quantity") )
                            .otherwise(None)
                        ).otherwise(col("sls_price"))
                   )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming column

# COMMAND ----------

rename_map = {
    "sls_ord_num": "order_number",
    "sls_prd_key": "product_number",
    "sls_cust_id": "customer_id",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price"
}
for old_name,new_name in rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking the dataframe

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing silver Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking the silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.crm_sales