#!/usr/bin/bash


spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.14 --master spark://PublicDNS:7077 --conf spark.dynamicAllocation.enabled=false --executor-memory 14G spark_processing.py