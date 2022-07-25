# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store taxi example notebook
# MAGIC 
# MAGIC This notebook illustrates the use of Feature Store to create a model that predicts NYC Yellow Taxi fares. It includes these steps:
# MAGIC 
# MAGIC - Compute and write features.
# MAGIC - Train a model using these features to predict fares.
# MAGIC - Evaluate that model on a new batch of data using existing features, saved to Feature Store.
# MAGIC 
# MAGIC ## Requirements
# MAGIC - Databricks Runtime for Machine Learning 8.3 or above. 
# MAGIC 
# MAGIC **Note:** This notebook is written to run with Databricks Runtime for Machine Learning 10.2 or above. If you are using Databricks Runtime for Machine Learning 10.1 or below, delete or comment out Cmd 19 and uncomment Cmd 20.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>

# COMMAND ----------

# MAGIC %md ## Compute features

# COMMAND ----------

# MAGIC %md #### Load the raw data used to compute features
# MAGIC 
# MAGIC Load the `nyc-taxi-tiny` dataset.  This was generated from the full [NYC Taxi Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) which can be found at `dbfs:/databricks-datasets/nyctaxi` by applying the following transformations:
# MAGIC 
# MAGIC 1. Apply a UDF to convert latitude and longitude coordinates into ZIP codes, and add a ZIP code column to the DataFrame.
# MAGIC 1. Subsample the dataset into a smaller dataset based on a date range query using the `.sample()` method of the Spark `DataFrame` API.
# MAGIC 1. Rename certain columns and drop unnecessary columns.
# MAGIC 
# MAGIC If you want to create this dataset from the raw data yourself, follow these steps:
# MAGIC 1. Run the Feature Store taxi example dataset notebook ([AWS](https://docs.databricks.com/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html)|[Azure](https://docs.microsoft.com/azure/databricks/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html)|[GCP](https://docs.gcp.databricks.com/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html)) to generate the Delta table.
# MAGIC 1. In this notebook, replace the following `spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")` with: `spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")`

# COMMAND ----------

raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(raw_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC From the taxi fares transactional data, we will compute two groups of features based on trip pickup and drop off zip codes.
# MAGIC 
# MAGIC #### Pickup features
# MAGIC 1. Count of trips (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 1. Mean fare amount (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 
# MAGIC #### Drop off features
# MAGIC 1. Count of trips (time window = 30 minutes)
# MAGIC 1. Does trip end on the weekend (custom feature using python code)
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_computation_v5.png"/>

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone


@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday
  
@udf(returnType=StringType())  
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df


# COMMAND ----------

# MAGIC %md ### Data scientist's custom code to compute features

# COMMAND ----------

def pickup_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the pickup_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    pickupzip_features = (
        df.groupBy(
            "pickup_zip", window("tpep_pickup_datetime", "1 hour", "15 minutes")
        )  # 1 hour window, sliding every 15 minutes
        .agg(
            mean("fare_amount").alias("mean_fare_window_1h_pickup_zip"),
            count("*").alias("count_trips_window_1h_pickup_zip"),
        )
        .select(
            col("pickup_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("mean_fare_window_1h_pickup_zip").cast(FloatType()),
            col("count_trips_window_1h_pickup_zip").cast(IntegerType()),
        )
    )
    return pickupzip_features
  
def dropoff_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the dropoff_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df,  ts_column, start_date, end_date
    )
    dropoffzip_features = (
        df.groupBy("dropoff_zip", window("tpep_dropoff_datetime", "30 minute"))
        .agg(count("*").alias("count_trips_window_30m_dropoff_zip"))
        .select(
            col("dropoff_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("count_trips_window_30m_dropoff_zip").cast(IntegerType()),
            is_weekend(col("window.end")).alias("dropoff_is_weekend"),
        )
    )
    return dropoffzip_features  

# COMMAND ----------

from datetime import datetime

pickup_features = pickup_features_fn(
    raw_data, ts_column="tpep_pickup_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)
dropoff_features = dropoff_features_fn(
    raw_data, ts_column="tpep_dropoff_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

# COMMAND ----------

display(pickup_features)

# COMMAND ----------

# MAGIC %md ### Use Feature Store library to create new feature tables 

# COMMAND ----------

# MAGIC %md First, create the database where the feature tables will be stored.

# COMMAND ----------

dbutils.widgets.text("fs_db_name","")

# COMMAND ----------

fs_db_name = dbutils.widgets.get("fs_db_name")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS $fs_db_name;

# COMMAND ----------

# MAGIC %md Next, create an instance of the Feature Store client.

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC Use either the `create_table` API (Databricks Runtime 10.2 ML or above) or the `create_feature_table` API (Databricks Runtime 10.1 ML or below) to define schema and unique ID keys. If the optional argument `df` (Databricks Runtime 10.2 ML or above) or `features_df` (Databricks Runtime 10.1 ML or below) is passed, the API also writes the data to Feature Store.

# COMMAND ----------

# This cell uses an API introduced with Databricks Runtime 10.2 ML.
# If your cluster is running Databricks Runtime 10.1 ML or below, skip or comment out this cell and uncomment and run Cmd 22.
'''
spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_table(
    name=f"{fs_db_name}.trip_pickup_features",
    primary_keys=["zip", "ts"],
    df=pickup_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Pickup Features",
)
fs.create_table(
    name=f"{fs_db_name}.trip_dropoff_features",
    primary_keys=["zip", "ts"],
    df=dropoff_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Dropoff Features",
)
'''

# COMMAND ----------

# To run this notebook with Databricks Runtime 10.1 ML or below, uncomment this cell.

spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_feature_table(
    name=f"{fs_db_name}.trip_pickup_features",
    keys=["zip", "ts"],
    features_df=pickup_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Pickup Features",
)

fs.create_feature_table(
    name=f"{fs_db_name}.trip_dropoff_features",
    keys=["zip", "ts"],
    features_df=dropoff_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Dropoff Features",
)

# COMMAND ----------

# MAGIC %md ## Update features
# MAGIC 
# MAGIC Use the `write_table` function to update the feature table values.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_compute_and_write.png"/>

# COMMAND ----------

display(raw_data)

# COMMAND ----------

# Compute the pickup_features feature group.
pickup_features_df = pickup_features_fn(
  df=raw_data,
  ts_column="tpep_pickup_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# Write the pickup features DataFrame to the feature store table
fs.write_table(
  name=f"{fs_db_name}.trip_pickup_features",
  df=pickup_features_df,
  mode="merge",
)

# Compute the dropoff_features feature group.
dropoff_features_df = dropoff_features_fn(
  df=raw_data,
  ts_column="tpep_dropoff_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# Write the dropoff features DataFrame to the feature store table
fs.write_table(
  name=f"{fs_db_name}.trip_dropoff_features",
  df=dropoff_features_df,
  mode="merge",
)

# COMMAND ----------

# MAGIC %md When writing, both `merge` and `overwrite` modes are supported.
# MAGIC 
# MAGIC     fs.write_table(
# MAGIC       name="feature_store_taxi_example.trip_pickup_features",
# MAGIC       df=pickup_features_df,
# MAGIC       mode="overwrite",
# MAGIC     )
# MAGIC     
# MAGIC Data can also be streamed into Feature Store by passing a dataframe where `df.isStreaming` is set to `True`:
# MAGIC 
# MAGIC     fs.write_table(
# MAGIC       name="streaming_example.streaming_features",
# MAGIC       df=streaming_df,
# MAGIC       mode="merge",
# MAGIC     )
# MAGIC     
# MAGIC You can schedule a notebook to periodically update features using Databricks Jobs ([AWS](https://docs.databricks.com/jobs.html)|[Azure](https://docs.microsoft.com/azure/databricks/jobs)|[GCP](https://docs.gcp.databricks.com/jobs.html)).

# COMMAND ----------

# MAGIC %md Analysts can interact with Feature Store using SQL, for example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(count_trips_window_30m_dropoff_zip) AS num_rides,
# MAGIC        dropoff_is_weekend
# MAGIC FROM   $fs_db_name.trip_dropoff_features
# MAGIC WHERE  dropoff_is_weekend IS NOT NULL
# MAGIC GROUP  BY dropoff_is_weekend;

# COMMAND ----------

# MAGIC %md ## Feature Search and Discovery

# COMMAND ----------

# MAGIC %md
# MAGIC You can now discover your feature tables in the <a href="#feature-store/" target="_blank">Feature Store UI</a>.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>
# MAGIC 
# MAGIC Search by "trip_pickup_features" or "trip_dropoff_features" to view details such as table schema, metadata, data sources, producers, and online stores. 
# MAGIC 
# MAGIC You can also edit the description for the feature table, or configure permissions for a feature table using the dropdown icon next to the feature table name. 
# MAGIC 
# MAGIC Check the [Use the Feature Store UI
# MAGIC ](https://docs.databricks.com/applications/machine-learning/feature-store.html#use-the-feature-store-ui) documentation for more details.

# COMMAND ----------

# MAGIC %md ## Train a model
# MAGIC 
# MAGIC This section illustrates how to train a model using the pickup and dropoff features stored in Feature Store. It trains a LightGBM model to predict taxi fare.

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
import math
from datetime import timedelta
import mlflow.pyfunc


def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).timestamp())


rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())


def rounded_taxi_data(taxi_data_df):
    # Round the taxi data timestamp to 15 and 30 minute intervals so we can join with the pickup and dropoff features
    # respectively.
    taxi_data_df = (
        taxi_data_df.withColumn(
            "rounded_pickup_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_pickup_datetime"], lit(15)),
        )
        .withColumn(
            "rounded_dropoff_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_dropoff_datetime"], lit(30)),
        )
        .drop("tpep_pickup_datetime")
        .drop("tpep_dropoff_datetime")
    )
    taxi_data_df.createOrReplaceTempView("taxi_data")
    return taxi_data_df
  
def get_latest_model_version(model_name):
  latest_version = 1
  mlflow_client = MlflowClient()
  for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
    version_int = int(mv.version)
    if version_int > latest_version:
      latest_version = version_int
  return latest_version

# COMMAND ----------

# MAGIC %md ### Read taxi data for training

# COMMAND ----------

taxi_data = rounded_taxi_data(raw_data)

# COMMAND ----------

# MAGIC %md ### Understanding how a training dataset is created
# MAGIC 
# MAGIC In order to train a model, you need to create a training dataset that is used to train the model.  The training dataset is comprised of:
# MAGIC 
# MAGIC 1. Raw input data
# MAGIC 1. Features from the feature store
# MAGIC 
# MAGIC The raw input data is needed because it contains:
# MAGIC 
# MAGIC 1. Primary keys used to join with features.
# MAGIC 1. Raw features like `trip_distance` that are not in the feature store.
# MAGIC 1. Prediction targets like `fare` that are required for model training.
# MAGIC 
# MAGIC Here's a visual overview that shows the raw input data being combined with the features in the Feature Store to produce the training dataset:
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_feature_lookup.png"/>
# MAGIC 
# MAGIC These concepts are described further in the Creating a Training Dataset documentation ([AWS](https://docs.databricks.com/applications/machine-learning/feature-store.html#create-a-training-dataset)|[Azure](https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/feature-store#create-a-training-dataset)|[GCP](https://docs.gcp.databricks.com/applications/machine-learning/feature-store.html#create-a-training-dataset)).
# MAGIC 
# MAGIC The next cell loads features from Feature Store for model training by creating a `FeatureLookup` for each needed feature.

# COMMAND ----------

# If you are running Databricks Runtime for Machine Learning 9.1 or below, you can uncomment the code in this cell and use it instead of the code in Cmd 34.

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = f"{fs_db_name}.trip_pickup_features"
dropoff_features_table = f"{fs_db_name}.trip_dropoff_features"

pickup_feature_lookups = [
    FeatureLookup( 
      table_name = pickup_features_table,
      feature_names = ["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
      lookup_key = ["pickup_zip", "rounded_pickup_datetime"],
    ),
]

dropoff_feature_lookups = [
    FeatureLookup( 
      table_name = dropoff_features_table,
      feature_names = ["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
      lookup_key = ["dropoff_zip", "rounded_dropoff_datetime"],
    ),
]

# COMMAND ----------

# MAGIC %md ### Create a Training Dataset
# MAGIC 
# MAGIC When `fs.create_training_set(..)` is invoked below, the following steps will happen:
# MAGIC 
# MAGIC 1. A `TrainingSet` object will be created, which will select specific features from Feature Store to use in training your model. Each feature is specified by the `FeatureLookup`'s created above. 
# MAGIC 
# MAGIC 1. Features are joined with the raw input data according to each `FeatureLookup`'s `lookup_key`.
# MAGIC 
# MAGIC The `TrainingSet` is then transformed into a DataFrame to train on. This DataFrame includes the columns of taxi_data, as well as the features specified in the `FeatureLookups`.

# COMMAND ----------

# End any existing runs (in the case this notebook is being run for a second time)
mlflow.end_run()

# Start an mlflow run, which is needed for the feature store to log the model
mlflow.start_run() 

# Since the rounded timestamp columns would likely cause the model to overfit the data 
# unless additional feature engineering was performed, exclude them to avoid training on them.
exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]

# Create the training set that includes the raw input data merged with corresponding features from both feature tables
training_set = fs.create_training_set(
  taxi_data,
  feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
  label = "fare_amount",
  exclude_columns = exclude_columns
)

# Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
training_df = training_set.load_df()

# COMMAND ----------

# Display the training dataframe, and note that it contains both the raw input data and the features from the Feature Store, like `dropoff_is_weekend`
display(training_df)

# COMMAND ----------

# MAGIC %md ## Next steps
# MAGIC 
# MAGIC 1. Explore the feature tables created in this example in the <a href="#feature-store">Feature Store UI</a>.
# MAGIC 1. Adapt this notebook to your own data and create your own feature tables.