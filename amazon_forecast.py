import sys
import os

import json
import util
import boto3
import s3fs
import pandas as pd

#Create an instance of AWS SK cliente for amazon forecast
region = 'us-east-2'
session = boto3.Session(region_name = region)
forecast = session.client(service_name = 'forecast')
forecastquery = session.client(service_name = 'forecastquery')

#Setup IAM role used by aws forecast to acces your data
role_name = "ForecastNotebookRole-Basic"
print(f"Creating Role {role_name}...")

role_arn = util.get_or_create_iam_role(role_name = role_name)
# echo user inputs without account
print(f"Success! Created role = {role_arn.split('/')[1]}")


# import data
key = "data/taxi-dec2017-jan2019.csv"

taxi_df = pd.read_csv(key, dtype = object, names=['timestamp','item_id','target_value'])

bucket_name = "forecast + FECHA RANGO"

s3 = boto3.Session().resource('S3')
bucket = s3.Bucket(bucket_name)
if not bucket.creation_date:
    if region != "us-east-2":
        s3.create_bucket(Bucket = bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    else:
        s3.create_bucket(Bucket = bucket_name)

s3.Bucket(bucket_name).Object(key).upload_file(key)
ts_s3_path = f"s3://{bucket_name}/{key}"

print(f"\nDone, the dataset is uploaded to S3 at {ts_s3_path}.")

## Crear el dataset para Amazon Forecast
DATASET_FREQUENCY = "D"
TS_DATASET_NAME = "input_target"
TS_SCHEMA = {
   "Attributes":[
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"target_value",
         "AttributeType":"integer"
      }
   ]
}

create_dataset_response = forecast.create_dataset(Domain = "CUSTOM",
                                                  DatasetType = "TARGET_TIME_SERIES",
                                                  DatasetName = TS_DATASET_NAME,
                                                  DataFrequency = DATASET_FREQUENCY,
                                                  Schema = TS_SCHEMA)

ts_dataset_arn = create_dataset_response['DatasetArn']
describe_dataset_response = forecast.describe_dataset(DatasetArn = ts_dataset_arn)
print(f"The Dataset with ARN {ts_dataset_arn} is now {describe_dataset_response['Status']}.")

### Importar el Dataset:
TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss" #revisar
TS_IMPORT_JOB_NAME = "TAXI_TSS_IMPORT"
TIMEZONE = "EST" #revisar

ts_dataset_import_job_response = \
   forecast.create_dataset_import_job(DatasetImportJobName = TS_IMPORT_JOB_NAME,
                                      DatasetArn = ts_dataset_arn,
                                      DataSource ={
                                          "S3Config": {
                                              "Path": ts_s3_path,
                                              "RoleArn": role_arn
                                          }
                                      },
                                      TimestampFormat = TIMESTAMP_FORMAT,
                                      TimeZone = TIMEZONE)

ts_dataset_import_job_arn = ts_dataset_import_job_response['DatasetImportJobArn']
describe_dataset_import_job_response = forecast.describe_dataset_import_job(DatasetImportJobArn=ts_dataset_import_job_arn)
print(f"Waiting for Dataset Import Job with ARN {ts_dataset_import_job_arn} to become ACTIVE. This process could take 5-10 minutes.\n\nCurrent Status:")

describe_dataset_import_job_response = forecast.describe_dataset_import_job(DatasetImportJobArn=ts_dataset_import_job_arn)
print(f"\n\nThe Dataset Import Job with ARN {ts_dataset_import_job_arn} is now {describe_dataset_import_job_response['Status']}.")

# CREATING A DATASET GROUP
DATASET_GROUP_NAME = "Forecast_Week + X"
DATASET_ARNS = [ts_dataset_arn]

create_dataset_group_response = 