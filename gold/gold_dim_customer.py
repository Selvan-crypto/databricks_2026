# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col,when

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation Logic

# COMMAND ----------

dim_customer = (spark.table("workspace.silver.crm_customer").alias("ci")
                .join(spark.table("workspace.silver.erp_customers").alias("ca"),
                      col("ci.customer_key") == col("ca.customer_number"),how = "inner")
                .join(spark.table("workspace.silver.erp_customer_location").alias("cl"),
                      col("ci.customer_key") == col("cl.customer_number"),how = "inner")
                .select(
                    F.row_number().over(Window.orderBy("ci.customer_id")).alias("customer_key"),
                    "ci.customer_id",
                    "cl.customer_number",
                    "ci.first_name",
                    "ci.last_name",
                    "cl.country",
                    "ci.marital_status",
                    when(col("ci.gender") != "n/a",col("ci.gender"))
                    .otherwise(F.coalesce(col("ca.gender"),F.lit("n/a"))).alias("gender"),
                    col("ca.birth_date").alias("birthdate"),
                    "ci.create_date"
                    )
                )
dim_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Gold Layer

# COMMAND ----------

dim_customer.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sanity check of gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_customer

# COMMAND ----------

