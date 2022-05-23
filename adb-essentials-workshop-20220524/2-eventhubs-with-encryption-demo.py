# Databricks notebook source
# MAGIC %md
# MAGIC ### This demo shows:
# MAGIC 
# MAGIC `Symmetric encryption on selected attribute in json payload`
# MAGIC 
# MAGIC `Partial encryption using a fernet key`
# MAGIC 
# MAGIC `Write payloads from spark df to EventHub`
# MAGIC 
# MAGIC `Read payload and decrypt flattened results into cleartext df`
# MAGIC 
# MAGIC Structure:
# MAGIC * Section 1: Eventhub connection and how to READ from Eventhub and operate on payload.
# MAGIC * Section 2: How to encrypt json payload and WRITE from databricks to Eventhub.
# MAGIC 
# MAGIC Prerequisite:
# MAGIC * Tested on `DBR 10.4 LTS`.
# MAGIC * Generate a fernet key and store it in AKV Secrets, create AKV backed secret scope on Databricks.
# MAGIC * Generate eventhub connection string and store it in AKV Secrets
# MAGIC 
# MAGIC Click RunAll, this demo is idempotent.

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF to do Symmetric Encryption using AKV stored secrets
# MAGIC * Using UDF and Fernet key for Column-level encryption -> https://databricks.com/notebooks/enforcing-column-level-encryption.html
# MAGIC * Relatively old method via symmetric encryption, same key for encryption and decryption

# COMMAND ----------

from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType

# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text
 
# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val

# Register UDF's
encrypt = udf(encrypt_val, StringType())
decrypt = udf(decrypt_val, StringType())

# Encrypt the data
#encrypted = df.withColumn("ssn", encrypt("ssn",lit(fernet_key)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions to generate random content -> encrypt partial json payload -> write to Eventhub
# MAGIC * Encryption can be done at json payload level, or at spark df level
# MAGIC * For simplicity, we used a fixed schema and hardcoded encryption attribute in payload json string level

# COMMAND ----------

import json, string, random
from random import randint

def get_encrypt_key(scope, keyname):
    # Fetch key from dbutils.secrets
    return dbutils.secrets.get(scope = scope, key = keyname)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def gen_single_json_str_payload(key="", encryption=True):
    # simulate sensitive content, encryption happen at payload level
    sensitive_key = 'name'
    sensitive_val = id_generator()
    if encryption:
        sensitive_val = encrypt_val(sensitive_val, key)
    payload_dict = {
      sensitive_key: sensitive_val,
      'age': randint(1, 100)
    }
    return json.dumps(payload_dict)
    
def gen_payload_df(payload_json_str):
    payload_df = spark.createDataFrame([(1, payload_json_str)],["id","body"])
    return payload_df

def write_df_eventhub(payload_df, ehConf):
    # ingest 1 row payload to EH
    payload_df.select("body").write.format("eventhubs").options(**ehConf).save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1 EventHub Config
# MAGIC * Connection String

# COMMAND ----------

# AKV params, replace with your values
scope = "eventhub"
fernet_encryption_keyname = "encryptionkeyfernet"
eh_connection_str_keyname = "ehConnectionStr"

# EH params, connectionString can be built using connectionStringBuilder
ehConf = {}
connectionString = dbutils.secrets.get(scope="eventhub", key=eh_connection_str_keyname)
ehConf['eventhubs.connectionString'] = connectionString
# Add consumer group to the ehConf dictionary
ehConf['eventhubs.consumerGroup'] = "$Default"
# Encrypt ehConf connectionString property
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 1.2 Start reading from Eventhub
# MAGIC * raw results will be stored in df_from_eventhub

# COMMAND ----------

# Read events from the Event Hub
df_from_eventhub = spark.readStream.format("eventhubs").options(**ehConf).load()
# Visualize the Dataframe in realtime
display(df_from_eventhub)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.3 Decode raw EH content (df_from_eventhub)
# MAGIC * Extract from "body" of the payload
# MAGIC * Decode is not decrypt, this step just decode the binary payload to string value
# MAGIC * After decoding, you retrieve the original message (still encrypted using your fernet key), then you can choose to use this encrypted value, or decrypt it

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F

payload_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

decoded_df = df_from_eventhub.select(F.from_json(F.col("body").cast("string"), payload_schema).alias("payload"))

# Visualize the transformed df
display(decoded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.4 Flatten json payload from decoded_df

# COMMAND ----------

df_events = decoded_df.select(F.col("payload.name").alias("name"), F.col("payload.age").alias("age"))

# COMMAND ----------

display(df_events)

# COMMAND ----------

fernet_key = get_encrypt_key(scope, fernet_encryption_keyname)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.5 Decrypt specific column of flattened df

# COMMAND ----------

# decrypt using udf
decrypted = df_events.withColumn("cleartext_name", decrypt("name",lit(fernet_key)))
display(decrypted)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 Write partially encrypted json payload to EventHub 

# COMMAND ----------

write_df_eventhub(gen_payload_df(gen_single_json_str_payload(fernet_key, True)), ehConf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Shut down all active streams
# MAGIC * Uncomment and run to stop all streams.

# COMMAND ----------

#for s in spark.streams.active:
#    s.stop()

# COMMAND ----------


