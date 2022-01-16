import datetime
import re
import sys
import time

import pandas as pd

from google.cloud import storage
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import FloatType, IntegerType, StringType
from typing import *


def clean_bit(val: str) -> int:
    '''Ensures valid bit format'''
    if not val or val.lower() == 'na':
        return None
    return int(val.lower() == 'yes')


def clean_date(date: str) -> str:
    '''Ensures valid date format'''
    try:
        return pd.to_datetime(date).strftime('%Y-%m-%d')
    except ValueError:
        return None


def clean_decimal(val: str) -> float:
    '''Ensures valid decimal format'''
    try:
        return float(val)
    except ValueError:
        return None


def clean_int(val: str) -> int:
    '''Ensures valid integer format'''
    try:
        return int(val)
    except ValueError:
        return None


def clean_location(val: str) -> str:
    '''...'''
    if not val or val.lower() == 'na':
        return None
    return val
    
    
def clean_wind_direction(direction: str) -> str:
    '''Ensures valid wind direction format'''
    if not direction or direction.lower() == 'na':
        return None
    direction = direction.replace(' ', '')
    return direction if len(direction) > 0 and all([c.lower() in 'nsew' for c in direction]) else None


def main():
    BUCKET_NAME =  sys.argv[1]
    TABLE_NAME = sys.argv[2]

    # create spark session
    spark = SparkSession.builder.appName('data_processiong').getOrCreate()

    # load data into dataframe if table exists
    try:
        df = spark.read.format('bigquery').option('table', TABLE_NAME).load()
    except Py4JJavaError as e:
        raise Exception(f'Error reading {TABLE_NAME}') from e

    # define the table schema and clean functions
    schema = {
          'Date': UserDefinedFunction(clean_date, StringType())
        , 'Location': UserDefinedFunction(clean_location, StringType())
        , 'MinTemp': UserDefinedFunction(clean_decimal, FloatType())
        , 'MaxTemp': UserDefinedFunction(clean_decimal, FloatType())
        , 'Rainfall': UserDefinedFunction(clean_decimal, FloatType())
        , 'Evaporation': UserDefinedFunction(clean_decimal, FloatType())
        , 'Sunshine': UserDefinedFunction(clean_int, IntegerType())
        , 'WindGustDir': UserDefinedFunction(clean_wind_direction, StringType())
        , 'WindGustSpeed': UserDefinedFunction(clean_int, IntegerType())
        , 'WindDir9am': UserDefinedFunction(clean_wind_direction, StringType())
        , 'WindDir3pm': UserDefinedFunction(clean_wind_direction, StringType())
        , 'WindSpeed9am': UserDefinedFunction(clean_int, IntegerType())
        , 'WindSpeed3pm': UserDefinedFunction(clean_int, IntegerType())
        , 'Humidity9am': UserDefinedFunction(clean_int, IntegerType())
        , 'Humidity3pm': UserDefinedFunction(clean_int, IntegerType())
        , 'Pressure9am': UserDefinedFunction(clean_decimal, FloatType())
        , 'Pressure3pm': UserDefinedFunction(clean_decimal, FloatType())
        , 'Cloud9am': UserDefinedFunction(clean_int, IntegerType())
        , 'Cloud3pm': UserDefinedFunction(clean_int, IntegerType())
        , 'Temp9am': UserDefinedFunction(clean_decimal, FloatType())
        , 'Temp3pm': UserDefinedFunction(clean_decimal, FloatType())
        , 'RainToday': UserDefinedFunction(clean_bit, IntegerType())
        , 'RainTomorrow': UserDefinedFunction(clean_bit, IntegerType())
    }

    for col, func in schema.items():
        df = df.withColumn(col, func(col))

    df.show(n = 20)

    if '--dry-run' in sys.argv:
        print('Data will not be uploaded to GCS')
    else:
        # set path to GCS
        path = str(time.time())
        temp_path = f'gs://{BUCKET_NAME}/{path}'

        # write dataframe to temp location to preserve tthe data in final location
        print('Uploading data to GCS...')
        (
            df.write
            .options(codec = 'org.apache.hadoop.io.compress.GzipCodec')
            .csv(temp_path)
        )

        # get GCS bucket
        print('Getting storage client...')
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(BUCKET_NAME)

        blobs = list(source_bucket.list_blobs(prefix = path))

        # copy files from temp location to the final location
        print('Copying from temp location...')
        final_path = 'clean_weather_data/'
        for blob in blobs:
            file_match = re.match(path + r"/(part-\d*)[0-9a-zA-Z\-]*.csv.gz", blob.name)
            if file_match:
                new_blob = final_path + file_match[1] + '.vsv.gz'
                source_bucket.copy_blob(blob, source_bucket, new_blob)

        # delete the temp location
        for blob in blobs:
            blob.delete()

        print(f'Data sucessfully uploaded to gs://{BUCKET_NAME}/{final_path}')


if __name__ == '__main__':
    main()
