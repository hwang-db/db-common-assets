# Databricks notebook source
# MAGIC 
# MAGIC %run ./configuration

# COMMAND ----------

import pandas as pd

# COMMAND ----------

people_df = pd.DataFrame(
    [
        {
            "name": "Lai Hui",
            "address": "805 John Oval Apt. 470\nLake Amanda, NE 09043",
            "phone_number": "3087607759",
            "user_id": "16b74cfe-d9da-11ea-8534-0242ac110002",
            "device_id": 1,
        },
        {
            "name": "Armando Clemente",
            "address": "293 Keith Drive\nEast David, NY 05983",
            "phone_number": "8497224309",
            "user_id": "16b78264-d9da-11ea-8534-0242ac110002",
            "device_id": 2,
        },
        {
            "name": "Meallan O'Conarain",
            "address": "3048 Guerrero Alley\nJerryhaven, PA 56888",
            "phone_number": "(580)703-9076x32254",
            "user_id": "16b79f9c-d9da-11ea-8534-0242ac110002",
            "device_id": 3,
        },
        {
            "name": "Lakesia Brown",
            "address": "549 Palmer Village\nLake Joseph, IN 44981",
            "phone_number": "001-624-908-5142x446",
            "user_id": "16b7b946-d9da-11ea-8534-0242ac110002",
            "device_id": 4,
        },
        {
            "name": "Anu Achaval",
            "address": "8334 Kevin Fork Suite 531\nSouth Kennethton, WI 42697",
            "phone_number": "468-733-3330x598",
            "user_id": "16b7cec2-d9da-11ea-8534-0242ac110002",
            "device_id": 5,
        },
        {
            "name": "Ae Yujin",
            "address": "USCGC Davis\nFPO AP 67548",
            "phone_number": "001-072-063-6894x746",
            "user_id": "16b7dd72-d9da-11ea-8534-0242ac110002",
            "device_id": 6,
        },
        {
            "name": "Pardeep Kapoor",
            "address": "653 Monica Knoll\nHicksfort, KS 41378",
            "phone_number": "001-834-698-3839x6306",
            "user_id": "16b7f0d2-d9da-11ea-8534-0242ac110002",
            "device_id": 7,
        },
        {
            "name": "Julian Andersen",
            "address": "321 Jackson Forest Apt. 689\nGarciafort, UT 91205",
            "phone_number": "001-796-472-0831x3399",
            "user_id": "16b807ac-d9da-11ea-8534-0242ac110002",
            "device_id": 8,
        },
        {
            "name": "Simone Graber",
            "address": "55863 Brown Cliff\nPort Amybury, ND 99197",
            "phone_number": "4548070215",
            "user_id": "16b81c2e-d9da-11ea-8534-0242ac110002",
            "device_id": 9,
        },
        {
            "name": "Gonzalo Valdés",
            "address": "2456 Rachael Manors Apt. 758\nSouth Curtisfort, WV 27129",
            "phone_number": "(408)059-4700x9591",
            "user_id": "16b830c4-d9da-11ea-8534-0242ac110002",
            "device_id": 10,
        },
    ]
)

# COMMAND ----------

people_spark_df = spark.createDataFrame(people_df)
(people_spark_df.write.format("delta").mode("overwrite").save(peopleDimPath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS health_tracker_user
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_user
USING DELTA
LOCATION "{peopleDimPath}"
"""
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS deletions
"""
)

spark.sql(
    f"""
CREATE TABLE deletions AS
SELECT user_id FROM health_tracker_user
WHERE user_id in (
  '16b807ac-d9da-11ea-8534-0242ac110002',
  '16b81c2e-d9da-11ea-8534-0242ac110002'
)
"""
)
