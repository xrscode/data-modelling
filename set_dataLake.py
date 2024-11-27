# Databricks notebook source
# DBTITLE 1,Description
"""
The purpose of this notebook is to ensure:
1. Necessary dependencies are installed. 
2. Access to blob storage is configured.
3. Correct folders exist in blob storage.
4. API key loaded
"""

# COMMAND ----------

# DBTITLE 1,Install Dependencies
import requests
import json
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, DateType

# COMMAND ----------

# DBTITLE 1,Security Variables
# Set Storage Variables
secret_scope         = "carbonScope"
storage_account_name = dbutils.secrets.get(scope=secret_scope, key="storageAccountName")
sas_token            = dbutils.secrets.get(scope=secret_scope, key="sasToken")
api_key              = dbutils.secrets.get(scope=secret_scope, key="apiKey")

# COMMAND ----------

# DBTITLE 1,Spark Config
# Configure Spark to Connect to Storage Account:
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS") 
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider") 
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token) 


# COMMAND ----------

# DBTITLE 1,Set DataLake and File Locations
# Set location to data lake and files:
DataLake             = dbutils.secrets.get(scope=secret_scope, key="DataLake")
sample_sales         = DataLake + "RAW/sales_data.csv"
products             = DataLake + 'RAW/products.csv'
schema               = DataLake + 'schema/'
