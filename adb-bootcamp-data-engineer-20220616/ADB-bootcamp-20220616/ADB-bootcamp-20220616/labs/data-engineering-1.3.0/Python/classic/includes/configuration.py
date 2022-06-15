# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# ANSWER
username = "dbacademy"

# COMMAND ----------

peopleDimPath = f"/dbacademy/{username}/dataengineering/people/"
classicPipelinePath = f"/dbacademy/{username}/dataengineering/classic/"

landingPath = classicPipelinePath + "landing/"
rawPath = classicPipelinePath + "raw/"
bronzePath = classicPipelinePath + "bronze/"
silverPath = classicPipelinePath + "silver/"
silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
goldPath = classicPipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities