# Databricks notebook source
# DBTITLE 1,Setup dataLake Connection
# MAGIC %run "../set_dataLake"

# COMMAND ----------

# DBTITLE 1,File
# Establish link to file:
public_sales = DataLake + 'RAW/Fact_Sales_1.csv'

# COMMAND ----------

# DBTITLE 1,Dataframe
# Read CSV and create dataframe:
df = spark.read.format('csv').option('header', 'true').load(public_sales)

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView('public_sales_tv')
