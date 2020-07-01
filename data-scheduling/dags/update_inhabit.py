#This script is written by Anvitha Kandiraju 
#This script has the schedule for Airflow to run tasks

#import libraries

from builtins import range
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os

#path to airflow scripts

srcDir = '/home/ubuntu/Python_Airflow_Scripts'

#spark-submit command

sparkSubmit = 'spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.14 --master spark://X.X.X.X:7077 --conf spark.dynamicAllocation.enabled=false --num-executors 3 --executor-memory 14G '

#default arguments for scheduling

default_args = {
        'owner': 'ubuntu',
        'depends_on_past': False,
        'start_date': datetime(2020,6,28),
        'retries': 5,
        'retry_delay': timedelta(minutes=1),
        }

dag = DAG('update_aq_s3',default_args=default_args,schedule_interval='@daily')

#script to perform task of updating openaq data

downloadAQData = BashOperator(
        task_id='update-aq-s3-data',
        bash_command=srcDir+'/update_openaq.sh ',
        dag=dag)
        
#script to perform task of updating NOAA data

downloadNOAAData = BashOperator(
        task_id='update-noaa-s3-data',
        bash_command=srcDir+'/update_noaa.sh ',
        dag=dag)

#script to perform task of running spark job to update db

sparkProcessData = BashOperator(
        task_id='process_data',
        bash_command=sparkSubmit+' ' + srcDir + '/spark_update.py',
        dag=dag)

# dependencies

downloadAQData >> sparkProcessData
downloadNOAAData >> sparkProcessData