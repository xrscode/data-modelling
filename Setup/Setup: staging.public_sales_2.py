# Databricks notebook source
"""
Notebook to run for first time setup.
"""

# COMMAND ----------

# DBTITLE 1,Setup dataLake Connection
# MAGIC %run "../set_dataLake"

# COMMAND ----------

# DBTITLE 1,File
# Establish link to file:
public_sales = DataLake + 'RAW/Fact_Sales_2.csv'

# COMMAND ----------

# DBTITLE 1,Dataframe
# Read CSV and create dataframe:
df = spark.read.format('csv').option('header', 'true').load(public_sales)

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView('public_sales_tv')

# COMMAND ----------

query = """
SELECT
transaction_id,
to_timestamp(transactional_date, 'M/d/yyyy H:mm') AS transactional_date,
product_id,
customer_id,
payment,
credit_card,
loyalty_card,
cost, 
quantity,
price

FROM public_sales_tv
"""
df_cleaned = spark.sql(query)
df_cleaned.createOrReplaceTempView('public_sales_cleaned_tv')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO staging.public_sales AS target
# MAGIC USING public_sales_cleaned_tv AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (target.transactional_date, target.product_id, target.customer_id, target.payment, target.credit_card, target.loyalty_card, target.cost, target.quantity, target.price)
# MAGIC      VALUES (source.transactional_date, source.product_id, source.customer_id, source.payment, source.credit_card, source.loyalty_card, source.cost, source.quantity, source.price);
