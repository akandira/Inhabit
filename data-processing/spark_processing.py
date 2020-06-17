#!/usr/bin/python
# -*- coding: utf-8 -*-
# This script is written by Anvitha Kandiraju for
# Insight Data Engineering project
# This script reads Amazon s3 bucket data on pollution and Noaa,
# processes the data and saves into postgres database

# import libraries

import json
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, \
    ArrayType, LongType, DoubleType, BooleanType
from pyspark.sql.functions import col, explode, to_date, mean, count, \
    lit, current_date, create_map

# create Spark Session

spark = SparkSession.builder.appName('InhabitProject'
        ).config('spark.default.parallelism', '400').getOrCreate()

# create Spark Context

sc = spark.sparkContext
sc.setLogLevel('WARN')

# postgreSQL parameters

pg_usr = os.getenv('PGSQL_USER')
pg_pw = os.getenv('PGSQL_PW')
url = 'jdbc:postgresql://10.0.0.9:5430/inhabit_db'


# function to define AQ dataset Schema

def get_AQSchema():

    schema_source = [StructField('name', StringType(), True),
                     StructField('url', StringType(), True)]
    schema_avgperiod = [StructField('unit', StringType(), True),
                        StructField('value', DoubleType(), True)]
    schema_coordinates = [StructField('latitude', DoubleType(), True),
                          StructField('longitude', DoubleType(), True)]
    schema_date = [StructField('local', StringType(), True),
                   StructField('utc', StringType(), True)]
    schema_air = StructType([
        StructField('attribution',
                    ArrayType(StructType(schema_source)), True),
        StructField('averagingPeriod', StructType(schema_avgperiod),
                    True),
        StructField('city', StringType(), True),
        StructField('coordinates', StructType(schema_coordinates),
                    True),
        StructField('country', StringType(), True),
        StructField('date', StructType(schema_date), True),
        StructField('location', StringType(), True),
        StructField('mobile', BooleanType(), True),
        StructField('parameter', StringType(), True),
        StructField('sourceName', StringType(), True),
        StructField('sourceType', StringType(), True),
        StructField('unit', StringType(), True),
        StructField('value', DoubleType(), True),
        ])
    return schema_air


# function to read Json file to Data frame

def read_jsonfile(file_path, input_schema):

    df = spark.read.json(file_path, schema=input_schema)
    return df


# function to read textfile

def read_textfile(file_path):

    df = spark.read.text(file_path)
    return df


# function to get selected fields from NOAA data

def select_NOAAdataset(df):

    df = df.select(
        df_noaa.value.substr(7, 8).alias('Date'),
        df_noaa.value.substr(23, 7).cast('double').alias('Longitude'),
        df_noaa.value.substr(31, 7).cast('double').alias('Latitude'),
        df_noaa.value.substr(63, 7).cast('double'
                ).alias('AirTempDailyAvg'),
        df_noaa.value.substr(71, 7).cast('double'
                ).alias('TotalDailyPrecip'),
        df_noaa.value.substr(79, 8).cast('double'
                ).alias('SolarRadiationDaily'),
        df_noaa.value.substr(106, 7).cast('double'
                ).alias('SurfaceTempDailyAvg'),
        df_noaa.value.substr(130, 7).cast('double').alias('RHDailyAvg'
                ),
        df_noaa.value.substr(138, 7).cast('double').alias('SoilMoist5cm'
                ),
        df_noaa.value.substr(154, 7).cast('double'
                ).alias('SoilMoist20cm'),
        df_noaa.value.substr(170, 7).cast('double'
                ).alias('SoilMoist50cm'),
        df_noaa.value.substr(178, 7).cast('double').alias('SoilTemp5cm'
                ),
        df_noaa.value.substr(194, 7).cast('double').alias('SoilTemp20cm'
                ),
        df_noaa.value.substr(210, 7).cast('double'
                ).alias('SoilTemp100cm'),
        )

    return df


# function to get selected fields from AQ data

def select_AQdataset(df):

    df = df.select(
        'coordinates.latitude',
        'coordinates.longitude',
        'date.utc',
        'parameter',
        'unit',
        'value',
        )
    return df


# function to delete Null Values

def delete_nullValues(df):

    df = df.dropna()
    return df


# function to check if values are in range

def cleanDf_Col(
    df,
    col_name,
    min_limit,
    max_limit,
    ):

    df_filtered = df.filter((col(col_name) < max_limit)
                            & (col(col_name) > min_limit))
    return df_filtered


# function to delete duplicate values

def delete_duplicate(df):

    df = df.dropDuplicates()
    return df


# function to delete negtive values

def delete_negative(df, col_name):

    df = df.filter(col(col_name) > 0)
    return df


# function to delete negtive values

def default_filter(df, col_name):

    default_valone = -99.0
    default_valtwo = -9999.0
    df = df.filter((col(col_name) != default_valone) & (col(col_name)
                   != default_valtwo))
    return df


# function to append table into dbtable

def append_table(
    df,
    url,
    dbtable,
    user_name,
    password,
    ):

    df.write.format('jdbc').mode('append').option('url',
            url).option('dbtable', dbtable).option('user',
            user_name).option('password', password).option('driver',
            'org.postgresql.Driver').save()
    return

# years of data available
    
years = [
    '2013',
    '2014',
    '2015',
    '2016',
    '2017',
    '2018',
    '2019',
    '2020',
    ]

for current_year in years:

    print ('Year: ' + str(current_year))

    # Processing of S3 Air quality data

    # S3 location for my data

    my_s3bucket_air = \
        's3a://pollutiondataset/DailyAirqualityData/{}*/*.ndjson'.format(current_year)

    # read all JSON files to a PySpark dataframe

    aq_schema = get_AQSchema()
    reading_start = time.time()
    df_air = read_jsonfile(my_s3bucket_air, aq_schema)
    dur = time.time() - reading_start
    print ('Reading AQ file takes in sec: ' + str(dur))

    start = time.time()

    # Filter required values

    df_air = select_AQdataset(df_air)

    # convert string to date format

    df_air = df_air.withColumn('utc', to_date(df_air.utc))

    # checks for data

    df_air = delete_nullValues(df_air)
    df_air = delete_duplicate(df_air)
    df_air = cleanDf_Col(df_air, 'latitude', -90, 90)
    df_air = cleanDf_Col(df_air, 'longitude', -180, 180)
    df_air = cleanDf_Col(df_air, 'utc', lit('2013-01-01'),
                         lit(current_date()))
    df_air = delete_negative(df_air, 'value')
    dur = time.time() - start
    print ('OpenAQ sorting, cleaning takes in sec: ' + str(dur))

    # Aggregate to daily data

    start = time.time()
    df_airagg = df_air.groupBy('latitude', 'longitude', 'parameter',
                               'unit', 'utc').agg(mean('value'
            ).alias('avg_value'))
    df_airagg = df_airagg.orderBy(df_airagg.utc.desc())
    dur = time.time() - start

    print ('OpenAQ aggregating takes in sec: ' + str(dur))

    # write air pollution data to database

    start = time.time()
    unique_params = df_airagg.select('parameter'
            ).distinct().rdd.flatMap(lambda x: x).collect()

    for param_idx in range(len(unique_params)):
        df_param = df_airagg.where(df_airagg['parameter']
                                   == unique_params[param_idx])
        append_table(df_param, url, unique_params[param_idx], pg_usr,
                     pg_pw)

    dur = time.time() - start
    print ('OpenAQ writing data takes in sec: ' + str(dur))

    # Processing of S3 Noaa data

    # S3 location for my data

    my_s3bucket_noaa = \
        's3a://noaaweatherdataset/NoaaData/{}/*.txt'.format(current_year)

    # read all text files to a PySpark dataframe

    start = time.time()
    df_noaa = read_textfile(my_s3bucket_noaa)
    df_noaa = select_NOAAdataset(df_noaa)
    dur = time.time() - start
    print ('NOAA Reading: ' + str(dur))

    # Select required parameters

    start = time.time()

    # change Date format

    df_noaa = df_noaa.withColumn('Date', to_date(df_noaa.Date,
                                 'yyyyMMdd'))

    # checks for errors

    df_noaa = delete_nullValues(df_noaa)
    df_noaa = delete_duplicate(df_noaa)
    df_noaa = cleanDf_Col(df_noaa, 'latitude', -90, 90)
    df_noaa = cleanDf_Col(df_noaa, 'longitude', -180, 180)
    df_noaa = cleanDf_Col(df_noaa, 'Date', lit('2013-01-01'),
                          lit(current_date()))
    dur = time.time() - start

    print ('All NOAA Processing: ' + str(dur))

    # writing Noaa data to data base

    start = time.time()

    noaadb_params = [
        'AirTempDailyAvg',
        'TotalDailyPrecip',
        'SolarRadiationDaily',
        'SurfaceTempDailyAvg',
        'RHDailyAvg',
        'SoilMoist5cm',
        'SoilMoist20cm',
        'SoilMoist50cm',
        'SoilTemp5cm',
        'SoilTemp20cm',
        'SoilTemp100cm',
        ]

    for w_param in noaadb_params:
        df_sel = df_noaa.select('Date', 'latitude', 'longitude',
                                w_param)
        df_sel = default_filter(df_sel, w_param)

        append_table(df_sel, url, w_param, pg_usr, pg_pw)

    dur = time.time() - start
    print ('Noaa writing data takes in sec: ' + str(dur))
