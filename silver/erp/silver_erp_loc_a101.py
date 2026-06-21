# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import trim,col,when
from pyspark.sql.types import StringType,DateType

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the data from Bronze Layer

# COMMAND ----------

df = spark.table("workspace.bronze.erp_loc_a101")

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
# MAGIC ## Customemr ID CleanUp

# COMMAND ----------

df =df.withColumn("CID",F.regexp_replace(col("CID"),"-",""))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Country Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before Normalization

# COMMAND ----------

df.select("CNTRY").distinct().show()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("CNTRY",
                   when(col("CNTRY") == "DE","Germany")
                   .when(col("CNTRY").isin("US",'USA'),"United States")
                   .when((col("CNTRY") == "") | (col("CNTRY").isNull()),"n/a")
                   .otherwise(col("CNTRY")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### After Normalization

# COMMAND ----------

df.select("CNTRY").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the column

# COMMAND ----------

rename_map = {
    "CID":"customer_number",
    "CNTRY":"country"
}
for old_name,new_name in rename_map.items():
    df =df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Dataframe

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the silver table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customer_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check of silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.erp_customer_location