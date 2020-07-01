# This script is written by Anvitha Kandiraju for
# Insight Data Engineering project
# This script reads Amazon S3 bucket data on pollution and NOAA,
# processes the data on a daily basis and saves into postgres database

# import libraries

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, \
     ArrayType, DoubleType, BooleanType
import pyspark.sql.functions as pyspark_acx
from datetime import date,timedelta

# create Spark Session

spark = SparkSession.builder.appName('InhabitProjectDailyRun'
        ).config('spark.executor.cores', 4).getOrCreate()


sc = spark.sparkContext
sc.setLogLevel('WARN')

# postgreSQL parameters

pg_usr = os.getenv('PGSQL_USER')
pg_pw = os.getenv('PGSQL_PW')
url = os.getenv('URL')
pg_table = os.getenv('PGSQL_TBNAME')


# function to define AQ dataset Schema
#output is schema for dataframe

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


# function to read Json file to data frame

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
        df_noaa.value.substr(7, 8).alias('utc'),
        df_noaa.value.substr(23, 7).cast('double').alias('longitude'),
        df_noaa.value.substr(31, 7).cast('double').alias('latitude'),
        df_noaa.value.substr(63, 7).cast('double'
                ).alias('airtemp'),
        df_noaa.value.substr(71, 7).cast('double'
                ).alias('totalprecip'),
        df_noaa.value.substr(79, 8).cast('double'
                ).alias('solarrad'),
        df_noaa.value.substr(106, 7).cast('double'
                ).alias('surftemp'),
        df_noaa.value.substr(130, 7).cast('double').alias('rh'
                ),
        df_noaa.value.substr(138, 7).cast('double').alias('sm5cm'
                ),
        df_noaa.value.substr(154, 7).cast('double'
                ).alias('sm20cm'),
        df_noaa.value.substr(170, 7).cast('double'
                ).alias('sm50cm'),
        df_noaa.value.substr(178, 7).cast('double').alias('st5cm'
                ),
        df_noaa.value.substr(194, 7).cast('double').alias('st20cm'
                ),
        df_noaa.value.substr(210, 7).cast('double'
                ).alias('st100cm'),
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

    df_filtered = df.filter((pyspark_acx.col(col_name) < max_limit)
                            & (pyspark_acx.col(col_name) > min_limit))
    return df_filtered


# function to delete duplicate values

def delete_duplicate(df):

    df = df.dropDuplicates()
    return df


# function to delete negtive(defaults) values

def delete_negative(df, col_name):

    df = df.filter(pyspark_acx.col(col_name) > 0)
    return df


# function to delete negtive(defaults) values

def default_filter(df, col_name):

    default_valone = -99.0
    default_valtwo = -9999.0
    df = df.filter((pyspark_acx.col(col_name) != default_valone) & (pyspark_acx.col(col_name)
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
    df.repartition(3).write.format('jdbc').mode('append').option('url',
            url).option('dbtable', dbtable).option('user',
            user_name).option('password', password).option('driver',
            'org.postgresql.Driver').option('batchsize',2000000).save()
    return


# obtaining date for processing:
# Scheduled to process 3 days old data to allow data upload at source

process_date = date.today()-timedelta(days=3)
year = process_date.year
month =  format(process_date.month, '02d')
day = format(process_date.day, '02d')


# Processing of S3 Air quality data

# S3 location for my data
# get data only for one day

my_s3bucket_air = \
    's3a://pollutiondataset/DailyAirqualityData/{}-{}-{}/*.ndjson'.format(year, month, day)


# read all JSON files to a PySpark dataframe

aq_schema = get_AQSchema()

df_air = read_jsonfile(my_s3bucket_air, aq_schema)


# Filter values to get only required columns

df_air = select_AQdataset(df_air)

# convert date string to date format

df_air = df_air.withColumn('utc', pyspark_acx.to_date(df_air.utc))

# checks for data

df_air = delete_nullValues(df_air)
df_air = delete_duplicate(df_air)
df_air = cleanDf_Col(df_air, 'latitude', -90, 90)
df_air = cleanDf_Col(df_air, 'longitude', -180, 180)
df_air = cleanDf_Col(df_air, 'utc', process_date-timedelta(days=2),
                      process_date+timedelta(days=2))
df_air = delete_negative(df_air, 'value')



# Aggregate to daily data


df_airagg = df_air.groupBy('latitude', 'longitude', 'parameter',
                           'unit', 'utc').agg(pyspark_acx.mean('value'
        ).alias('avg_value'))




# write air pollution data to database


append_table(df_airagg.select("utc","latitude","longitude","parameter","avg_value","unit"), url, pg_table, pg_usr, pg_pw)



# Processing of S3 NOAA data

# S3 location for my data

my_s3bucket_noaa = \
    's3a://noaaweatherdataset/NoaaData/{}/*.txt'.format(year)


# read all text files to a PySpark dataframe

df_noaa = read_textfile(my_s3bucket_noaa)

# Select required parameters

df_noaa = select_NOAAdataset(df_noaa)


# change Date format

df_noaa = df_noaa.withColumn('utc', pyspark_acx.to_date(df_noaa.utc,
                             'yyyyMMdd'))

# checks for errors

df_noaa = delete_nullValues(df_noaa)
df_noaa = delete_duplicate(df_noaa)
df_noaa = cleanDf_Col(df_noaa, 'latitude', -90, 90)
df_noaa = cleanDf_Col(df_noaa, 'longitude', -180, 180)

# get filter data for one day of processing
min_date = process_date-timedelta(days=1)
max_date = process_date+timedelta(days=1)
df_noaa = cleanDf_Col(df_noaa, 'utc', min_date, \
                      max_date)



# Change data format to get data similar to AQ data for writing in same DB table
# which require transposing columns to rows

# Add mapcol with following parameters in one column

df_noaa = df_noaa.withColumn('mapCol', \
                           pyspark_acx.create_map(pyspark_acx.lit('airtemp_celsius'),df_noaa.airtemp,
                                      pyspark_acx.lit('totalprecip_mm'),df_noaa.totalprecip,
                                      pyspark_acx.lit('solarrad_MJ/m2'),df_noaa.solarrad,
                                      pyspark_acx.lit('surftemp_celsius'),df_noaa.surftemp,
                                      pyspark_acx.lit('rh_%'),df_noaa.rh,
                                      pyspark_acx.lit('sm5cm_m3/m3'),df_noaa.sm5cm,
                                      pyspark_acx.lit('sm20cm_m3/m3'),df_noaa.sm20cm,
                                      pyspark_acx.lit('sm50cm_m3/m3'),df_noaa.sm50cm,
                                      pyspark_acx.lit('st5cm_celsius'),df_noaa.st5cm,
                                      pyspark_acx.lit('st20cm_celsius'),df_noaa.st20cm,
                                      pyspark_acx.lit('st100cm_celsius'),df_noaa.st100cm
                                     )
                    )

#explode mapcol to transpose column to rows
df_noaaagg = df_noaa.select('*',pyspark_acx.explode(df_noaa.mapCol).alias('parameter_unit','avg_value'))

#Split parameter_unit coloumn to parameter and unit coloumns
split_col = pyspark_acx.split(df_noaaagg['parameter_unit'], '_')
df_noaaagg = df_noaaagg.withColumn('parameter', split_col.getItem(0))
df_noaaagg = df_noaaagg.withColumn('unit', split_col.getItem(1))

#drop transposed columns
columns_to_drop =['airtemp','totalprecip','solarrad','surftemp_celsius','rh',
                  'sm5cm','sm20cm','sm50cm','st5cm','st20cm','st100cm','parameter_unit','surftemp','mapCol']
df_noaaagg = df_noaaagg.drop(*columns_to_drop)

# remove rows with default values
df_noaaagg = default_filter(df_noaaagg,'avg_value')
#df_noaaagg.show(10)

# create/append NOAA data to DB table
append_table(df_noaaagg.select("utc","latitude","longitude","parameter","avg_value","unit"), url,pg_table, pg_usr, pg_pw)