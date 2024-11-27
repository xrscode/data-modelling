# Databricks notebook source
# MAGIC %sql
# MAGIC -- Setup schema for staging:
# MAGIC DROP SCHEMA IF EXISTS staging CASCADE;
# MAGIC CREATE SCHEMA staging;
# MAGIC
# MAGIC --Setup schema for core:
# MAGIC DROP SCHEMA IF EXISTS core CASCADE;
# MAGIC CREATE SCHEMA core;
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- Setup up staging.sales table:
# MAGIC CREATE TABLE IF NOT EXISTS staging.sales (
# MAGIC   transaction_id INT,
# MAGIC   transactional_date TIMESTAMP,
# MAGIC   product_id STRING,
# MAGIC   customer_id INT,
# MAGIC   payment STRING,
# MAGIC   credit_card BIGINT,
# MAGIC   loyalty_card STRING,
# MAGIC   cost DECIMAL(10,2),
# MAGIC   quantity INT,
# MAGIC   price DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC -- Setup up core.sales fact table:
# MAGIC CREATE TABLE IF NOT EXISTS core.sales (
# MAGIC   transaction_id INT,
# MAGIC   transactional_date TIMESTAMP,
# MAGIC   transactional_date_fk BIGINT,
# MAGIC   product_id STRING,
# MAGIC   product_fk INT,
# MAGIC   customer_id INT,
# MAGIC   payment_fk INT,
# MAGIC   credit_card BIGINT,
# MAGIC   cost DECIMAL(10,2),
# MAGIC   quantity INT,
# MAGIC   price DECIMAL(10,2),
# MAGIC   total_cost DECIMAL(10,2),
# MAGIC   total_price DECIMAL(10,2),
# MAGIC   profit DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC -- Setup up core.dim_payment dimension table:
# MAGIC CREATE TABLE IF NOT EXISTS core.dim_payment (
# MAGIC   payment_pk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   payment STRING,
# MAGIC   loyalty_card STRING
# MAGIC );
# MAGIC
# MAGIC --Setup staging.dim_product dimension table:
# MAGIC CREATE TABLE IF NOT EXISTS staging.dim_product(
# MAGIC   product_pk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   product_id STRING,
# MAGIC   product_brand STRING,
# MAGIC   category STRING,
# MAGIC   sub_category STRING
# MAGIC );
# MAGIC
# MAGIC --Setup staging.fact_sales table:
# MAGIC CREATE TABLE IF NOT EXISTS staging.fact_sales (
# MAGIC   ORDERNUMBER INT, 
# MAGIC   QUANTITYORDERED INT, 
# MAGIC   PRICEEACH DOUBLE, 
# MAGIC   ORDERLINENUMBER INT, 
# MAGIC   SALES DOUBLE, 
# MAGIC   ORDERDATE STRING, 
# MAGIC   STATUS STRING, 
# MAGIC   QTR_ID INT, 
# MAGIC   MONTH_ID INT, 
# MAGIC   YEAR_ID INT, 
# MAGIC   PRODUCTLINE STRING, 
# MAGIC   MSRP INT, 
# MAGIC   PRODUCTCODE STRING, 
# MAGIC   CUSTOMERNAME STRING, 
# MAGIC   PHONE STRING, 
# MAGIC   ADDRESSLINE1 STRING, 
# MAGIC   ADDRESSLINE2 STRING, 
# MAGIC   CITY STRING, 
# MAGIC   STATE STRING, 
# MAGIC   POSTALCODE STRING, 
# MAGIC   COUNTRY STRING, 
# MAGIC   TERRITORY STRING, 
# MAGIC   CONTACTLASTNAME STRING, 
# MAGIC   CONTACTFIRSTNAME STRING
# MAGIC );
# MAGIC
