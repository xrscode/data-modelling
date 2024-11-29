# Databricks notebook source
# MAGIC %run "../set_dataLake"

# COMMAND ----------

# DBTITLE 1,Read Dataframe
# Read CSV and create dataframe:
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products)
# Cache dataframe:
df.cache()
# Rename column:
df = df.withColumnRenamed('product (brand)', 'product_brand')

# COMMAND ----------

# DBTITLE 1,Create Temp View
# Create temp view 'products':
df.createOrReplaceTempView("products")

# COMMAND ----------

# DBTITLE 1,Merge Data into dim.product
# MAGIC %sql
# MAGIC MERGE INTO staging.dim_product AS target
# MAGIC USING products AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (target.product_id, target.product_brand, target.category, target.sub_category) VALUES (source.product_id, source.product_brand, source.category, source.sub_category);

# COMMAND ----------

# DBTITLE 1,Remove from Cache
# Remove df from memory:
df.unpersist()
