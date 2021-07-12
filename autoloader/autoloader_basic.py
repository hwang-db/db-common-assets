# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC 
# MAGIC Contact: hao.wang@databricks.com
# MAGIC 
# MAGIC This tutorial shows how to use Databricks autoloader to ingest streaming data into delta tables, contains following topics:
# MAGIC 1. Connect to ADLS.
# MAGIC 2. Use autoloader to ingest and transform data on the fly, using pyspark / sql.
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader

# COMMAND ----------

# MAGIC %md ## 1. Connect to ADLS using key (non prod setup)

# COMMAND ----------

# this requires setup on secrets scope and key vault instance, replace hwangstorage1 with your storage account name
spark.conf.set(
    "fs.azure.account.key.hwangstorage1.dfs.core.windows.net",
    dbutils.secrets.get(scope="hwang-kv2",
    key="hwang-adls-access-key"))

# COMMAND ----------

# MAGIC %md ## 2. Autoloader 20 minute tutorial
# MAGIC 
# MAGIC Tips:
# MAGIC 1. New file with different name will trigger loading action
# MAGIC 2. Overwritten file in blob with the same name will not trigger loading action
# MAGIC 3. In any container we can't have 2 files with same name -> will trigger overwritten if same name
# MAGIC 4. You need to specify checkpoint path for autoloader
# MAGIC 5. You need to specify schema when loading input json sources (dynamic schema handle for another session)

# COMMAND ----------

# DBTITLE 1,Method 1: Directly transform streaming df and write to delta, for simple transformations
from pyspark.sql.types import *
from delta.tables import *
import pyspark.sql.functions as F

source_container_path = "abfss://autoloadersource@hwangstorage1.dfs.core.windows.net/"
sink_container_path = "abfss://autoloader@hwangstorage1.dfs.core.windows.net/testsink"
sink_checkpoint_path = "abfss://autoloader@hwangstorage1.dfs.core.windows.net/checkpoint"
schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

df_stream_in = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("maxFilesPerTrigger", 4) \
  .schema(schema_json) \
  .load(source_container_path)

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

# DBTITLE 1,Method 2: Using foreachbatch and function to process each microbatch df and update delta files
from pyspark.sql.types import *
from delta.tables import *
import pyspark.sql.functions as F

source_container_path = "abfss://autoloadersource@hwangstorage1.dfs.core.windows.net/"
sink_container_path = "abfss://sink3@hwangstorage1.dfs.core.windows.net/testsink"
sink_container_path_2 = "abfss://sink3@hwangstorage1.dfs.core.windows.net/testsink2"
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

# MAGIC %md ## Create managed delta table in dbfs
# MAGIC 
# MAGIC If we are to create external delta table in ADLS, we need to mount storage onto ADLS first.
# MAGIC Here we show how to directly create managed internal table.

# COMMAND ----------

# DBTITLE 1,Method 3: Foreachbatch and merge microbatch df into to delta table to update records
from pyspark import Row

source_container_path = "abfss://autoloadersource@hwangstorage1.dfs.core.windows.net/"
sink_container_path = "abfss://sink2@hwangstorage1.dfs.core.windows.net/testsink"
schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

# Function to upsert `microBatchOutputDF` into Delta table using MERGE
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

# create empty delta table
schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'
df = sqlContext.createDataFrame(sc.emptyRDD(), schema_json)
df.write \
  .format("delta").mode("overwrite").saveAsTable("aggregates")

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

# MAGIC %md ## Create external delta table in adls
# MAGIC 
# MAGIC Now we will show how to mount cloud storage account to your databricks workspace.
# MAGIC The purpose is to directly save table data into storage account.

# COMMAND ----------

# DBTITLE 1,Mount blob storage to store your table data
storageName = 'hwangstorage1'
accessKey = ''

# mount blob storage container mntcontainer
try:
  dbutils.fs.mount(
    source = "wasbs://mntcontainer@"+storageName+".blob.core.windows.net/",
    mount_point = "/mnt/test_tables",
    extra_configs = {"fs.azure.account.key."+storageName+".blob.core.windows.net":
                     accessKey})

except Exception as e:
  import re
  result = re.findall(r"^\s*Caused by:\s*\S+:\s*(.*)$", e.message, flags=re.MULTILINE)
  if result:
    print(result[-1]) # Print only the relevant error message
  else:
    print(e) # Otherwise print the whole stack trace.

# COMMAND ----------

# DBTITLE 1,Create your external delta table (data stored in storage account)
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS test_ext_table_1 (Country STRING, Language STRING, TweetDataVolume STRING)
# MAGIC   LOCATION '/mnt/test_tables/test_ext_table_1'

# COMMAND ----------

source_container_path = "abfss://autoloadersource@hwangstorage1.dfs.core.windows.net/"
schema_json = 'Country STRING, Language STRING, TweetDataVolume STRING'

# Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batchId): 
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("microbatchrows")
  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  microBatchOutputDF._jdf.sparkSession().sql("""
    MERGE INTO test_ext_table_1 t
    USING microbatchrows s
    ON s.Country = t.Country
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

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
# MAGIC select * from test_ext_table_1

# COMMAND ----------

# MAGIC %md ## Conclusion
# MAGIC Suggestion: Mount ADLS as your external delta table location.
# MAGIC Use the template above to update your delta table.
# MAGIC You can write output to multiple delta tables, as shown in method 2 of writing output to multiple locations.

# COMMAND ----------


