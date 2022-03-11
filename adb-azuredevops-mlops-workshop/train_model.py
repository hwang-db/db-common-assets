# Databricks notebook source
# MAGIC %md
# MAGIC #### Prepare variables for train_model notebook
# MAGIC 
# MAGIC Assumption: You have already created all required features in feature engineering step.
# MAGIC This notebook will form train_df and start mlflow tracking for your model training process.
# MAGIC We assume users are allowed to register trained models into model registry staging, while working in development env.
# MAGIC trigger again

# COMMAND ----------

dbutils.widgets.dropdown("register_to_staging", "True", ["True", "False"], "Register model to staging")
dbutils.widgets.text("raw_data_path", "/databricks-datasets/nyctaxi-with-zipcodes/subsampled", "Raw path for data source")
# you should also parameterize the usage of feature tables etc, for simplicity we skip those
# get var to decide if register to staging registry
register_staging = dbutils.widgets.get("register_to_staging")
raw_data_path = dbutils.widgets.get("raw_data_path")

# COMMAND ----------

# DBTITLE 1,Helper functions to prepare train_df
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

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone

raw_data = spark.read.format("delta").load(raw_data_path)
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

# If you are running Databricks Runtime for Machine Learning 9.1 or above, you can uncomment the code in this cell and use it instead of the code in Cmd 34.

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = "feature_store_taxi_example.trip_pickup_features"
dropoff_features_table = "feature_store_taxi_example.trip_dropoff_features"

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

# MAGIC %md
# MAGIC Train a LightGBM model on the data returned by `TrainingSet.to_df`, then log the model with `FeatureStoreClient.log_model`. The model will be packaged with feature metadata.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import numpy as np

def train_model(taxi_data, pickup_feature_lookups, dropoff_feature_lookups):
    # Start an mlflow run, which is needed for the feature store to log the model
    # end any active run
    mlflow.end_run()
    mlflow.set_experiment("/Repos/Development/mlops/train_model")
    
    with mlflow.start_run(run_name="Basic RF Experiment") as run:
        exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]
        fs = feature_store.FeatureStoreClient()
        # Create the training set that includes the raw input data merged with corresponding features from both feature tables
        training_set = fs.create_training_set(
          taxi_data,
          feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
          label = "fare_amount",
          exclude_columns = exclude_columns
        )

        # Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
        training_df = training_set.load_df()
        features_and_label = training_df.columns
        # Collect data into a Pandas array for training
        data = training_df.toPandas()[features_and_label]
        
        # force dropna -> in reality do proper processing work!
        data_dropna = data.dropna().reset_index().drop("index", axis=1)
        
        # pandas df input
        train, test = train_test_split(data_dropna, test_size=0.2, random_state=0)
        X_train = train.drop(["fare_amount"], axis=1)
        X_test = test.drop(["fare_amount"], axis=1)

        sc = StandardScaler()
        X_train = sc.fit_transform(X_train)
        X_test = sc.transform(X_test)
        
        y_train = train.fare_amount
        y_test = test.fare_amount

        regressor = RandomForestRegressor(n_estimators=20, random_state=0)
        regressor.fit(X_train, y_train)
        y_pred = regressor.predict(X_test)

        print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, y_pred))
        print('Mean Squared Error:', metrics.mean_squared_error(y_test, y_pred))
        print('Root Mean Squared Error:', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))

        mse = metrics.mean_squared_error(y_test, y_pred)
        # Log model
        mlflow.sklearn.log_model(regressor, "random-forest-model")
        # Log metrics
        mlflow.log_metric("mse", mse)

        runID = run.info.run_id
        experimentID = run.info.experiment_id

        print(f"Inside MLflow Run with run_id `{runID}` and experiment_id `{experimentID}`")

        if register_staging == "True":
            # Log the trained model with MLflow and package it with feature lookup information.
            fs.log_model(
              regressor,
              artifact_path="model_packaged",
              flavor=mlflow.sklearn,
              training_set=training_set,
              registered_model_name="taxi_example_fare_packaged"
            )
    return regressor

# COMMAND ----------

model = train_model(taxi_data, pickup_feature_lookups, dropoff_feature_lookups)

# COMMAND ----------


