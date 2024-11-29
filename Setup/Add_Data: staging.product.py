# Databricks notebook source
# DBTITLE 1,Max PK
# Query current state of core.products to determine max pk value:
query = """
SELECT Product_Pk, Product_Id 
FROM core.products
WHERE Product_Pk = (SELECT MAX(Product_Pk) FROM core.products)

"""
# Create dataframe:
df = spark.sql(query)
# Extract max value and increment by 1 to get new value:
new_pk = df.collect()[0][0] + 1
# Extract Product_Id
new_product_id = 'P' + str(int(df.collect()[0][1][1:])+1)

# COMMAND ----------

# DBTITLE 1,Add New Values to Staging
# Add 10 new values:
for i in range(10):
    # Generate new pk value:
    pk = new_pk + i
    # Generate new id value:
    id = 'P' + str(int(new_product_id[1:])+i)
    # Dynamic query:
    query = f"""
    INSERT INTO staging.dim_product(product_id, product_brand, category, sub_category)
    VALUES ('{id}', 'Apple', 'Computing', 'Laptops')
    """
    print(query)
    # Execute query:
    spark.sql(query)
   
