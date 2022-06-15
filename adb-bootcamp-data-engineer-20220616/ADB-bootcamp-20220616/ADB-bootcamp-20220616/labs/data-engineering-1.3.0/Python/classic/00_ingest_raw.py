# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw Data Generation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest data from a remote source into our source directory, `rawPath`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration
# MAGIC 
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC `includes/configuration`, e.g.
# MAGIC 
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Function
# MAGIC 
# MAGIC Run the following command to load the utility function, `retrieve_data`.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate User Table
# MAGIC 
# MAGIC Run the following cell to generate a User dimension table.

# COMMAND ----------

# MAGIC %run ./includes/user

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(classicPipelinePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Lab Data
# MAGIC 
# MAGIC Run this cell to prepare the data we will use for this Lab.

# COMMAND ----------

prepare_activity_data(landingPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion function
# MAGIC The `includes/utilities` file contains a function called `ingest_classic_data()`.
# MAGIC We will use this function to ingest an hour of data at a time.
# MAGIC This will simulate a Kafka feed.
# MAGIC 
# MAGIC Run this function here to ingest the first hour of data.
# MAGIC 
# MAGIC If successful, you should see the result, `True`.

# COMMAND ----------

ingest_classic_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data Directory
# MAGIC You should see that one file has landed in the Raw Data Directory.

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **EXERCISE:** Land five hours of data using the utility function,
# MAGIC `ingest_classic_data`.
# MAGIC 
# MAGIC 😎 **Note** the function can take a number of `hours` as an argument.

# COMMAND ----------

# ANSWER
ingest_classic_data(hours=5)

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print the Contents of the Raw Files
# MAGIC **EXERCISE**: Add the correct file paths to display the contents of the two raw files you loaded.

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/dbacademy/dbacademy/dataengineering/classic/raw/")[0].path
    )
)

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/dbacademy/dbacademy/dataengineering/classic/raw/")[1].path
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## What do you notice about the data? (scroll down)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC The `device_id` for `"name":"Gonzalo Valdés"` was passed as a uuid, when it should have been passed
# MAGIC as a string-encoded integer.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>