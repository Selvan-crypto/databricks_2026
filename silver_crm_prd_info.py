# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType,IntegerType,DateType
from pyspark.sql.functions import col,trim,substring,length,when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_prd_info")
df.display()

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

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Cleanup

# COMMAND ----------

df = df.withColumn("prd_cost",F.coalesce(col("prd_cost"),F.lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## product line normalization

# COMMAND ----------

df = df.withColumn(
    "prd_line",
    when(F.upper(col("prd_line")) == "R", "Road")
    .when(F.upper(col("prd_line")) == "M" ,"Mountain")
    .when(F.upper(col("prd_line")) == "S", "Other Sales")
    .when(F.upper(col("prd_line")) == "T", "Touring")
    .otherwise("n/a")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Casting

# COMMAND ----------

df = df.withColumn("prd_start_dt",F.date_format(col("prd_start_dt"),"dd-MM-yyyy"))
df = df.withColumn(
    "prd_end_dt",
    F.when(F.col("prd_end_dt").isNotNull(), 
           F.date_format(F.col("prd_end_dt").cast(DateType()), "dd-MM-yyyy"))
    .otherwise(F.col("prd_end_dt"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a cate_id column from prd_key column

# COMMAND ----------

df = df.withColumn("cate_id",F.regexp_replace(substring(col("prd_key"),1,5),"-","_"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modifying the prd_key column

# COMMAND ----------

df = df.withColumn("prd_key",substring(col("prd_key"),7,length(col("prd_key"))))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Columns

# COMMAND ----------

rename_map = {
    "cate_id":"category_id",
    "prd_id": "product_id",
    "prd_key": "product_number",
    "prd_nm":"product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt":"start_date",
    "prd_end_dt":"end_date"
}

for old_name,new_name in rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check of DataFrame

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Silver Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking of silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.crm_products