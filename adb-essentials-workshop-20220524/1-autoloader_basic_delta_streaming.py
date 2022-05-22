# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC 
# MAGIC This tutorial shows how to use Databricks autoloader to ingest streaming data into delta tables, contains following topics:
# MAGIC 1. Connect to ADLS.
# MAGIC 2. Use autoloader to ingest and transform data on the fly, using pyspark / sql.
# MAGIC 3. Use autoloader to update delta table (3.1 for managed delta table)
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader

# COMMAND ----------

# MAGIC %md ## 1. Connect to ADLS using key (non prod setup)
# MAGIC 
# MAGIC See this tutorial to set up your env:
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started

# COMMAND ----------

# this requires setup on secrets scope and key vault instance
spark.conf.set(
    "fs.azure.account.key.teststoragehwang2.dfs.core.windows.net",
    "adls-key")

# COMMAND ----------

# MAGIC %md ## 2. Autoloader quick tutorial
# MAGIC 
# MAGIC Tips:
# MAGIC 1. New file with different name will trigger loading action
# MAGIC 2. Overwritten file in blob with the same name will not trigger loading action
# MAGIC 3. In any container we can't have 2 files with same name -> will trigger overwritten if same name
# MAGIC 4. You need to specify checkpoint path for autoloader
# MAGIC 5. You need to specify schema when loading input json sources (dynamic schema handle for another session)

# COMMAND ----------

dbutils.widgets.text("storage_account","teststoragehwang2")
dbutils.widgets.text("source_container","autoloadersource")

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
source_container = dbutils.widgets.get("source_container")

# COMMAND ----------

# DBTITLE 1,Scenario 1: Directly transform streaming df and write to delta, for simple transformations
from pyspark.sql.types import *
from delta.tables import *
import pyspark.sql.functions as F

# scenario 1 using sink 1
source_container_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/"
sink_container_path = f"abfss://sink1@{storage_account}.dfs.core.windows.net/testsink"
sink_checkpoint_path = f"abfss://sink1@{storage_account}.dfs.core.windows.net/checkpoint"

schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

df_stream_in = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("maxFilesPerTrigger", 4) \
    .schema(schema_json) \
    .load(source_container_path)

# COMMAND ----------

# directly transform streaming df
df_transform = df_stream_in.withColumn("new_col", F.lit("1"))

# then write to delta table
df_transform.writeStream.format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", sink_checkpoint_path) \
  .start(sink_container_path)

# COMMAND ----------

loaded_df = spark.read.format("delta").load(sink_container_path)

# COMMAND ----------

loaded_df.show()

# COMMAND ----------

# DBTITLE 1,Scenario 2: Using foreachbatch and function to process each microbatch df and update delta files
from pyspark.sql.types import *
from delta.tables import *
import pyspark.sql.functions as F

source_container_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/"

# write out to multiple sinks: testsink and testsink2
sink_container_path = f"abfss://sink2@{storage_account}.dfs.core.windows.net/testsink"
sink_container_path_2 = f"abfss://sink2@{storage_account}.dfs.core.windows.net/testsink2"

schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

df_stream_in = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .schema(schema_json) \
    .load(source_container_path)

def add_new_col(df_in, batchId):
    # this can be your custom function to process microbatch streaming df
    # if you are writing output to multiple locations, cache the microbatch df and unpersist it
    df_in.cache()

    # you can perform multiple different transformations in the function and write output to different locations
    df_transformed = df_in.withColumn("fn_mod_col", F.lit("modified"))
    df_transformed_2 = df_in.withColumn("fn_mod_col_2", F.lit("second output location"))

    df_transformed.write \
    .partitionBy("Country","Language") \
    .format("delta") \
    .mode("append") \
    .save(sink_container_path)

    df_transformed_2.write \
    .partitionBy("Country","Language") \
    .format("delta") \
    .mode("append") \
    .save(sink_container_path_2)

    df_in.unpersist()

# using function and foreachbatch to process streaming in df
df_output = df_stream_in.writeStream \
    .outputMode("update") \
    .foreachBatch(add_new_col) \
    .start()

# COMMAND ----------

# check results, multiple modification by custom function
sink_df_1 = spark.read.format("delta").load(sink_container_path)
sink_df_2 = spark.read.format("delta").load(sink_container_path_2)

# COMMAND ----------

display(sink_df_1)

# COMMAND ----------

display(sink_df_2)

# COMMAND ----------

# DBTITLE 1,Scenario 3.1: Update records using foreachbatch and merge microbatch, using managed delta table (data in dbfs)
from pyspark import Row

# using sink3 container!
source_container_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/"
sink_container_path = f"abfss://sink3@{storage_account}.dfs.core.windows.net/testsink"

schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

# Function to upsert `microBatchOutputDF` into Delta table using MERGE INTO
def upsertToDelta(microBatchOutputDF, batchId): 
    # Set the dataframe to view name
    microBatchOutputDF.createOrReplaceTempView("microbatchrows")
    # Use the view name to apply MERGE
    # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
    microBatchOutputDF._jdf.sparkSession().sql("""
        MERGE INTO aggregates t
        USING microbatchrows s
        ON s.Country = t.Country
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# first to create empty delta table
schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'
df = sqlContext.createDataFrame(sc.emptyRDD(), schema_json)
df.write.format("delta").mode("overwrite").saveAsTable("aggregates")

# Define the streaming in df
df_stream_in = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .schema(schema_json) \
    .load(source_container_path)

# Start the query to continuously upsert into aggregates tables in update mode
df_stream_in.writeStream \
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE aggregates;
# MAGIC SELECT * FROM aggregates;

# COMMAND ----------

# stop all streams
for s in spark.streams.active:
    print(s)
    s.stop()
