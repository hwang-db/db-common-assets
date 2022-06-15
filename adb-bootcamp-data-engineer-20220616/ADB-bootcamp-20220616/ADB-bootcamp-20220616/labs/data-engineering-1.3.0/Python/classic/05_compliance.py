# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Complying with GDPR and CCPA
# MAGIC 
# MAGIC Occasionally, Moovio customers decide that they do not want their
# MAGIC personal data stored in your system. This information would usually
# MAGIC come from a web form, or an email list stored in a database, or another
# MAGIC way. In this lab, we will just work with two customers' uuids for
# MAGIC illustrative purposes.
# MAGIC 
# MAGIC In the case of static (non-streaming) tables, deleting data is
# MAGIC relatively simple and straightforward. In this lab, we will
# MAGIC delete select customers' data from static Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Delete customer data using `MERGE`
# MAGIC 1. Verify the deletions using Delta table history
# MAGIC 1. Use Time Travel to rollback a deletion
# MAGIC 1. Vacuum the table to complete the deletion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Raw to Bronze to Silver Delta Architecture

# COMMAND ----------

# MAGIC %run ./04_main

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Delete data - simple deletion from static tables
# MAGIC 
# MAGIC In the Moovio data ecosystem, the following customers
# MAGIC have requested to have their data deleted per GDPR and CCPA
# MAGIC 
# MAGIC - `'16b807ac-d9da-11ea-8534-0242ac110002'`
# MAGIC - `'16b81c2e-d9da-11ea-8534-0242ac110002'`
# MAGIC 
# MAGIC These user ids are available in the table `deletions`:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT (*)
FROM deletions
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Spark SQL to Manipulate the Delta tables
# MAGIC We will delete these users from our table by merging the
# MAGIC `deletions` table with the following tables:
# MAGIC 
# MAGIC - `health_tracker_user`
# MAGIC - `health_tracker_classic_bronze`
# MAGIC - `health_tracker_classic_silver`

# COMMAND ----------

spark.sql(
    """
MERGE INTO health_tracker_classic_bronze
USING deletions
ON health_tracker_classic_bronze.value RLIKE deletions.user_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TEMPORARY VIEW deletion_users AS
  SELECT health_tracker_user.user_id, device_id FROM
  deletions JOIN health_tracker_user
  ON deletions.user_id = health_tracker_user.user_id
"""
)

spark.sql(
    """
MERGE INTO health_tracker_classic_silver
USING deletion_users
ON deletion_users.device_id = health_tracker_classic_silver.device_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

spark.sql(
    """
MERGE INTO health_tracker_user
USING deletions
ON deletions.user_id = health_tracker_user.user_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

display(
    spark.sql(
        """
DESCRIBE HISTORY health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise:** Display the Count as of the Previous Version
# MAGIC 
# MAGIC Reference the table history from the above cell to identify the previous version
# MAGIC number. Use it to query the count of the `health_tracker_classic_silver`
# MAGIC as of that version.

# COMMAND ----------

# ANSWER
display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver VERSION AS OF 4
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Exercise:** Display the current count of `health_tracker_classic_silver`.

# COMMAND ----------

# ANSWER
display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rollback
# MAGIC 
# MAGIC Oops! You found out that one of the customers
# MAGIC was incorrectly deleted in that last commit. You need to revert
# MAGIC to the previous version of the table for that customer's records
# MAGIC only, while keeping the changes made for the other customer.
# MAGIC 
# MAGIC 🆘 Recall that `delete` removes the data from the latest version
# MAGIC of the Delta table but does not remove it from the physical
# MAGIC storage until the old versions are explicitly vacuumed.
# MAGIC This will allow us to restore the data we just deleted, but means
# MAGIC that we are not yet in compliance. We will address this later.

# COMMAND ----------

spark.sql(
    """
INSERT INTO health_tracker_classic_silver
SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
WHERE name = 'Simone Graber'
"""
)

# COMMAND ----------

display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

display(
    spark.sql(
        """
DESCRIBE HISTORY health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check that the customer's data has been restored:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver
WHERE name = 'Simone Graber'
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure that the other customer's data is still deleted:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver
WHERE name = 'Julian Andersen'
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vacuum
# MAGIC 
# MAGIC If we query an earlier version of the `health_tracker_classic_silver`
# MAGIC table, we can see that the customer's data still exists, and we
# MAGIC are therefore not yet actually in compliance.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
WHERE name in ('Julian Andersen', 'Simone Graber')
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use the VACUUM command to remove the old files.
# MAGIC The VACUUM command recursively vacuums directories associated
# MAGIC  with the Delta table and removes files that are no longer in
# MAGIC  the latest state of the transaction log for that table and
# MAGIC  that are older than a retention threshold. The default threshold
# MAGIC is 7 days.

# COMMAND ----------

from pyspark.sql.utils import IllegalArgumentException

try:
    spark.sql(
        """
    VACUUM health_tracker_classic_silver RETAIN 0 Hours
    """
    )
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC In order to remove the files in question immediately, we set the
# MAGIC retention period to 0 hours. It is not recommended to set a
# MAGIC retention interval shorter than 7 days, and as we see from the
# MAGIC error message, there are safeguards in place to prevent this
# MAGIC operation from succeeding.
# MAGIC 
# MAGIC We will set Delta to allow this operation.

# COMMAND ----------

spark.sql(
    """
SET spark.databricks.delta.retentionDurationCheck.enabled = false
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run the vacuum command:

# COMMAND ----------

spark.sql(
    """
VACUUM health_tracker_classic_silver RETAIN 0 Hours
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now when we attempt to query an earlier version, we get an error.
# MAGIC The files from the earlier version have been removed.

# COMMAND ----------

# ANSWER
# Uncomment and run this query
# display(
#     spark.sql(
#         """
# SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
# """
#     )
# )


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>