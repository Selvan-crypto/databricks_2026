# Databricks notebook source
# MAGIC %md
# MAGIC Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType,DateType
from pyspark.sql.functions import trim,col,when,substring,length

# COMMAND ----------

# MAGIC %md
# MAGIC # Read from Bronze Layer

# COMMAND ----------

df = spark.table('workspace.bronze.erp_cust_az12')

# COMMAND ----------

# MAGIC %md
# MAGIC # Tranformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trimming

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType,StringType):
        df = df.withColumn(field.name,trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer ID CleanUp

# COMMAND ----------

df = df.withColumn("CID",
                   F.when(col("CID").startswith("NAS"),
                        substring(col("CID"),4,length(col("CID"))))
                        .otherwise(col("CID"))
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date formating

# COMMAND ----------

df = df.withColumn("BDate",
                   F.date_format((col("BDate").cast(DateType())),"dd-MM-yyyy"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gender Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before normalization

# COMMAND ----------

df.select("GEN").distinct().show()

# COMMAND ----------

df = df.withColumn("GEN",
                   when(F.upper(col("GEN")).isin("M","MALE"),"Male")
                   .when(F.upper(col("GEN")).isin("F","FEMALE"),"Female")
                   .otherwise("n/a"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### After normalization

# COMMAND ----------

df.select("GEN").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming column name

# COMMAND ----------

rename_map = {
    "CID":"customer_number",
    "BDate":"birth_date",
    "GEN":"gender"
}

for old_name ,new_name in rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the DataFrame

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing a silver table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check of silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.erp_customers