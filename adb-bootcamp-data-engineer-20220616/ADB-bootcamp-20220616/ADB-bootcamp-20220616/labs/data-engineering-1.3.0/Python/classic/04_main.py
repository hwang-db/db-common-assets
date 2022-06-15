# Databricks notebook source
# MAGIC 
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

ingest_classic_data(hours=1)

rawDF = read_batch_raw(spark, rawPath)
transformedRawDF = transform_raw(rawDF)
rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_ingestdate"
)

rawToBronzeWriter.save(bronzePath)

dbutils.fs.rm(rawPath, recurse=True)

bronzeDF = read_batch_bronze(spark, bronzePath)
transformedBronzeDF = transform_bronze(bronzeDF)

(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(
    transformedBronzeDF
)

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanDF, partition_column="p_eventdate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

silverCleanedDF = repair_quarantined_records(
    spark, bronzeTable="health_tracker_classic_bronze", userTable="health_tracker_user"
)

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanedDF, partition_column="p_eventdate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanedDF, "loaded")
