#!/bin/bash

# bash file to download Noaa data and sync with s3 bucket

# specify folder for download
cd /home/ubuntu/NOAA_SRDB/

# link for ftp website
prefix="ftp://ftp.ncdc.noaa.gov/pub/data/uscrn/products/daily01/"

#suffix for link to go to specific year
year=$(date +"%Y")

#name of the directory  link+suffix year
dir_name="${prefix}${year}"

#download directory
wget --continue --mirror "${dir_name}/*.txt"

#source to sync with AWS
folder='/home/ubuntu/NOAA_SRDB/ftp.ncdc.noaa.gov/pub/data/uscrn/products/daily01'
source="${folder}/${year}"

#AWS destination folder to sync with
s3_buc='s3://noaaweatherdataset/NoaaData/'
destination="${s3_buc}${year}"

#sync with AWS
aws s3 sync $source $destination