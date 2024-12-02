# Databricks notebook source
# MAGIC %md
# MAGIC **DIM_PAYMENT**

# COMMAND ----------

# DBTITLE 1,dim_payment
query = """
SELECT DISTINCT
    COALESCE(payment, 'cash') AS payment
,   loyalty_card
FROM staging.public_sales
ORDER BY payment, loyalty_card
"""
df = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView("payment_tv")

# COMMAND ----------

# DBTITLE 1,Merge Into dim_payment
# MAGIC %sql
# MAGIC MERGE INTO core.dim_payment AS target
# MAGIC USING payment_tv AS source
# MAGIC ON target.payment = source.payment AND target.loyalty_card = source.loyalty_card
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (target.payment, target.loyalty_card) VALUES (source.payment, source.loyalty_card);

# COMMAND ----------

# MAGIC %md
# MAGIC **FACT_SALES**

# COMMAND ----------

query = """
SELECT 
    staging.public_sales.transaction_id
,   CAST(staging.public_sales.transactional_date AS INT)
,   CONCAT(YEAR(staging.public_sales.transactional_date), LPAD(MONTH(staging.public_sales.transactional_date), 2, '0'), LPAD(DAY(staging.public_sales.transactional_date), 2, '0')) AS transactional_date_fk
,   CAST(core.dim_payment.payment_pk AS INT) AS payment_pk
,   CAST(staging.public_sales.cost AS DECIMAL(10,2)) AS cost
,   core.products.product_id
,   staging.public_sales.customer_id
,   CAST(staging.public_sales.credit_card AS INT)
,   staging.public_sales.quantity
,   CAST(staging.public_Sales.price AS DECIMAL(10, 2)) AS price
,   CAST(staging.public_sales.quantity * staging.public_sales.price AS DECIMAL(10, 2)) AS total_price
,   CAST(staging.public_sales.quantity * staging.public_sales.cost AS DECIMAL(10,2)) AS total_cost 
,   CAST(total_price - total_cost AS DECIMAL(10,2)) AS profit
FROM staging.public_sales
LEFT JOIN core.dim_payment 
    ON core.dim_payment.payment = COALESCE(staging.public_sales.payment, 'cash') 
    AND core.dim_payment.loyalty_card = staging.public_sales.loyalty_card
LEFT JOIN core.products ON staging.public_sales.product_id = core.products.product_id
ORDER BY transaction_id
"""
df = spark.sql(query)
display(df)

# COMMAND ----------

# DBTITLE 1,Temp View
df.createOrReplaceTempView("fact_sales_tv")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.fact_sales AS target
# MAGIC USING fact_sales_tv AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (target.transaction_id, target.transactional_date, target.transactional_date_fk, target.payment_pk, target.cost, target.product_id, target.customer_id, target.credit_card, target.quantity, target.price, target.total_price, target.total_cost, target.profit) VALUES (source.transaction_id, source.transactional_date, source.transactional_date_fk, source.payment_pk, source.cost, source.product_id, source.customer_id, source.credit_card, source.quantity, source.price, source.total_price, source.total_cost, source.profit);
