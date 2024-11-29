# Databricks notebook source
# MAGIC %run "../set_dataLake"

# COMMAND ----------

# Read CSV and create dataframe:
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sample_sales)
# Cache dataframe:
df.cache()

# COMMAND ----------

# Create temp view 'products':
df.createOrReplaceTempView("sales_tv")
# Unpersist dataframe:
df.unpersist()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO staging.fact_sales AS target
# MAGIC USING sales_tv AS source
# MAGIC ON target.ORDERNUMBER = source.ORDERNUMBER
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (target.ORDERNUMBER, 
# MAGIC     target.QUANTITYORDERED,  
# MAGIC     target.PRICEEACH,
# MAGIC     target.ORDERLINENUMBER,  
# MAGIC     target.SALES,
# MAGIC     target.ORDERDATE,
# MAGIC     target.STATUS, 
# MAGIC     target.QTR_ID,  
# MAGIC     target.MONTH_ID,  
# MAGIC     target.YEAR_ID,  
# MAGIC     target.PRODUCTLINE,
# MAGIC     target.MSRP,  
# MAGIC     target.PRODUCTCODE,
# MAGIC     target.CUSTOMERNAME,
# MAGIC     target.PHONE,
# MAGIC     target.ADDRESSLINE1,
# MAGIC     target.ADDRESSLINE2,
# MAGIC     target.CITY,
# MAGIC     target.STATE,
# MAGIC     target.POSTALCODE,
# MAGIC     target.COUNTRY,
# MAGIC     target.TERRITORY,
# MAGIC     target.CONTACTLASTNAME,
# MAGIC     target.CONTACTFIRSTNAME) 
# MAGIC   VALUES 
# MAGIC     (source.ORDERNUMBER, 
# MAGIC   source.QUANTITYORDERED,  
# MAGIC   source.PRICEEACH,
# MAGIC   source.ORDERLINENUMBER,  
# MAGIC   source.SALES,
# MAGIC   source.ORDERDATE,
# MAGIC   source.STATUS, 
# MAGIC   source.QTR_ID,  
# MAGIC   source.MONTH_ID,  
# MAGIC   source.YEAR_ID,  
# MAGIC   source.PRODUCTLINE,
# MAGIC   source.MSRP,  
# MAGIC   source.PRODUCTCODE,
# MAGIC   source.CUSTOMERNAME,
# MAGIC   source.PHONE,
# MAGIC   source.ADDRESSLINE1,
# MAGIC   source.ADDRESSLINE2,
# MAGIC   source.CITY,
# MAGIC   source.STATE,
# MAGIC   source.POSTALCODE,
# MAGIC   source.COUNTRY,
# MAGIC   source.TERRITORY,
# MAGIC   source.CONTACTLASTNAME,
# MAGIC   source.CONTACTFIRSTNAME);
