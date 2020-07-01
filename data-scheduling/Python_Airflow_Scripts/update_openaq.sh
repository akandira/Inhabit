#!/bin/bash

# This script is written to sync openaq s3 bucket with source

#data source 

openaq="s3://openaq-fetches/realtime/"

#data destination 

my_s3_bucket="s3://pollutiondataset/DailyAirqualityData/"

# get yesterday date

yesterday_fldr=`date --date='yesterday' +%F`

#get day before yesterdays date

dby_fldr=`date --date='2 days ago' +%F`

#adding yesterday date suffix to path

yday_path_aq="${openaq}${yesterday_fldr}"
yday_path_mypath="${my_s3_bucket}${yesterday_fldr}"

#adding day before yesterday suffix to path

dbyday_path_aq="${openaq}${dby_fldr}"
dbyday_path_mypath="${my_s3_bucket}${dby_fldr}"

# first sync yesterday's folder

aws s3 sync ${yday_path_aq} ${yday_path_mypath}

# second make sure day before yesterday's folder is up to date

aws s3 sync ${dbyday_path_aq} ${dbyday_path_mypath}
