# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import trim,col,when
from pyspark.sql.types import StringType,IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC # Read from Bronze Table

# COMMAND ----------

df = spark.table('workspace.bronze.erp_px_cat_g1v2')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trimming

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType,StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalization maintenance flag to boolean

# COMMAND ----------

df = df.withColumn("MAINTENANCE",
                   when(F.upper(col("MAINTENANCE")) == "YES",F.lit("True"))
                   .when(F.upper(col("MAINTENANCE")) == "NO",F.lit("False"))
                   .otherwise("None")
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Column Name

# COMMAND ----------

rename_map = {
    "id":"category_id",
    "cat":"category",
    "subcat":"subcategory",
    "maintenance":"maintenace_flag"
}
for old_name,new_name in rename_map.items():
    df = df.withColumnRenamed(old_name,new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking of DataFrame

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Silver Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_product_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkinng the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.erp_product_category