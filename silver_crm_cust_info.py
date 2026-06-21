# Databricks notebook source
# MAGIC %md
# MAGIC # init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import trim,col,when
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,FloatType,DoubleType,DateType

# COMMAND ----------

rename_map = {
    "cst_id":"customer_id",
    "cst_key":"customer_key",
    "cst_firstname":"first_name",
    "cst_lastname":"last_name",
    "cst_marital_status":"marital_status",
    "cst_gndr":"gender",
    "cst_create_date":"create_date"
}

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Reading from Bronze Table

# COMMAND ----------

df = spark.table('workspace.bronze.crm_cust_info')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data _Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trimming

# COMMAND ----------

for field in df.schema.fields:
  if isinstance(field.dataType,StringType):
      df = df.withColumn(field.name,trim(col(field.name))) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalization

# COMMAND ----------

df = df.withColumn("cst_gndr",when(col("cst_gndr") == "M","Male").when(col("cst_gndr") == "F","Female").otherwise("n/a"))
df = df.withColumn("cst_marital_status",when(col("cst_marital_status") == "S","Single").when(col("cst_marital_status") == "M","Married").otherwise("n/a"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Renaming the column name

# COMMAND ----------

for old_name,new_name in rename_map.items():
  df = df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Casting

# COMMAND ----------

df = df.withColumn("create_date",col("create_date").cast(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write into silver layer

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable('workspace.silver.crm_customer')

# COMMAND ----------

# MAGIC %md
# MAGIC ## check the silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.crm_customer