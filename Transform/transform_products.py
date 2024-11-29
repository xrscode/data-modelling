# Databricks notebook source
# MAGIC %run "../set_dataLake"

# COMMAND ----------

# DBTITLE 1,Query
# Select values where the product_pk in staging is greater than the product_pk in core.
query = """
SELECT
    product_pk AS Product_Pk,
    product_id AS Product_Id,
    SUBSTRING_INDEX(TRIM(REPLACE(product_brand, '\t', '')), '(', 1) AS Product,
    SUBSTRING(product_brand, INSTR(product_brand, "(") + 1, INSTR(product_brand, ")") - INSTR(product_brand, "(") - 1) AS Brand,
    category AS Category,
    sub_category AS Subcategory
FROM staging.dim_product
WHERE product_pk > (
    SELECT COALESCE(MAX(product_pk), 0)
    FROM core.products
)
"""
df = spark.sql(query)
display(df)

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView("dim_product")

# COMMAND ----------

# DBTITLE 1,Update core.products
# MAGIC %sql
# MAGIC MERGE INTO core.products AS target
# MAGIC USING dim_product AS source
# MAGIC ON target.Product_Pk = source.Product_Pk
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET target.Product_Id = source.Product_Id,
# MAGIC                target.Product = source.Product,
# MAGIC                target.Brand = source.Brand,
# MAGIC                target.Category = source.Category,
# MAGIC                target.Subcategory = source.Subcategory
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (Product_Pk ,Product_Id, Product, Brand, Category, Subcategory)
# MAGIC     VALUES (source.Product_Pk, source.Product_Id, source.Product, source.Brand, source.Category, source.Subcategory);

# COMMAND ----------

# DBTITLE 1,DataLake
# Create dataframe:
prod_df = spark.sql("SELECT * FROM CORE.PRODUCTS")

# Write to datalake:
prod_df.write.format("parquet").mode("overwrite").save(prod_core)

