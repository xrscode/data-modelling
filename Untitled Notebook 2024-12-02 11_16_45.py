# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT SUBSTRING('dog(cat)!', INSTR('dog(cat)!', "(") + 1, INSTR('dog(cat)!', ")") - INSTR('dog(cat)!', "("))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM core.products
