# Databricks notebook source
# MAGIC %md
# MAGIC ### raw to bronze
# MAGIC This notebook will read from raw landing zone, and ingest the files into bronze layer delta table.
# MAGIC 
# MAGIC Bronze layer will be the single source of truth layer.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('raw_location','')
dbutils.widgets.text('db_name','testdb')
dbutils.widgets.text('bronzePath','')

raw_location = dbutils.widgets.get('raw_location')
db_name = dbutils.widgets.get('db_name')
bronzePath = dbutils.widgets.get('bronzePath')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit

raw_df = spark.read.json(raw_location)

raw_df_with_metadata = raw_df.select(
    "name",
    "age",
    lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
)

raw_df_with_metadata = raw_df_with_metadata.select("*", 
                            col("ingestdate").alias("p_ingestdate"))

# COMMAND ----------

raw_df_with_metadata.write.format("delta").mode("append").partitionBy("p_ingestdate").save(bronzePath)

# COMMAND ----------

sdf = spark.read.format('delta').load(bronzePath)

# COMMAND ----------

print(sdf.count())