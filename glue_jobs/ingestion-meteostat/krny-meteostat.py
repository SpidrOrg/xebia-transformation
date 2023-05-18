import json
import boto3
import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from meteostat import Point, Monthly
from io import StringIO
import sys
import logging
from awsglue.utils import getResolvedOptions


#opening sesion
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
glue = boto3.client('glue')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#getting data incremental
today = datetime.today()
Point.cache_dir = '/tmp'
Monthly.cache_dir = '/tmp'

try:
    #getting data from environment variable
    args = getResolvedOptions(sys.argv, ["bucket","gluejobname","mapper","folder","file_name"]) #folder updation
    bucket_name = args["bucket"]
    gluejobname = args["gluejobname"]
    mapped_station_by_city = args["mapper"]
    folder = args["folder"] #folder updation
    file_name = args["file_name"]
except Exception as err:
    logger.error(f"Error while reading environmental variables : {err}")
    sys.exit(0)


#reading mapper
finalweatherst_df = pd.read_csv(mapped_station_by_city,index_col=0)
finalweatherst_df=pd.DataFrame(finalweatherst_df)
data=list()
wlist=[]
folders = []
result = s3_client.list_objects(Bucket=bucket_name, Prefix=folder, Delimiter='/')
for prefix in result.get('CommonPrefixes', list()):
    p1 = prefix.get('Prefix', '')
    folders.append(p1)
if not folders:
    five_yrs_ago = datetime.now() + relativedelta(years=-5)
else:
    five_yrs_ago = datetime.now() + relativedelta(months=-3)
    
    
    
def weatherdata_fetch(df):
    for i in range(len(df)):
        station=df['StationID'][i]
        data = Monthly(station, start = five_yrs_ago, end = today)
        data = data.fetch()
        data['stationID']=df['StationID'][i]
        wlist.append(data)
        final_data = pd.concat(wlist)
    return final_data
finalweatherdata_df = weatherdata_fetch(finalweatherst_df)
    
    
#Parsing it into S3 bucket
now = datetime.now()
date_time = now.strftime("%Y-%m-%d")
filename = folder + date_time + '/' + file_name + '.csv'
csv_buffer = StringIO()
finalweatherdata_df.to_csv(csv_buffer)
try:
    s3_client.put_object(Bucket=bucket_name, ContentType='text/csv', Key=filename, Body=csv_buffer.getvalue())
except Exception as err:
    logger.error(f"Error while saving : {err}")
        
try:
    #glue job invocation
    runId = glue.start_job_run(JobName=gluejobname)
    status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
    print("Job Status : ", status['JobRun']['JobRunState'])
except Exception as err:
    logger.error(f"Error while starting glue job : {err}")
    

    
