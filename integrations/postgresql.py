# Databricks notebook source
# MAGIC %md ### Prerequisite
# MAGIC 
# MAGIC Azure Postgresql database, using flexible server setup.
# MAGIC A table called company had been created in default db postgres and ingested with 1 row of record.

# COMMAND ----------

driver = "org.postgresql.Driver"
dbname = "guest"
user = "azureuser@hwangpsqldemo"
password = ""
query = "select * from company"

# url directs to single server of azure postgresql
url = f"jdbc:postgresql://hwangpsqldemo.postgres.database.azure.com:5432/{dbname}?user=azureuser@hwangpsqldemo&password={password}&sslmode=require"

# url_flexible directs to flexible server of azure postgresql, using default db postgres
url_flexible = f"jdbc:postgresql://hwangpsqldemoflexbile.postgres.database.azure.com:5432/postgres?user=azureuser&password={password}&sslmode=require"

# url_flex_pgbouncer directs to flexible server with pgbouncer enabled, using default db postgres
url_flex_pgbouncer = f"jdbc:postgresql://hwangpsqldemoflexbile.postgres.database.azure.com:6432/postgres?user=azureuser&password={password}&sslmode=require"

# COMMAND ----------

remote_table = spark.read.format("jdbc") \
   .option("driver", driver) \
   .option("url", url_flex_pgbouncer) \
   .option("user", user) \
   .option("password", password) \
   .option("query", query) \
   .option("ssl", True) \
   .option("sslmode", "require" ) \
   .load()

# COMMAND ----------

remote_table.show()

# COMMAND ----------


