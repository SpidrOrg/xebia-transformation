# -*- coding: utf-8 -*-
"""
Short Desc: This programe is a ETL Glue Job for kearney sensing solution

This scripts reads the raw data from source path on S3 bucket
and do the merging and cleaning of files and save in cleaned data path
and apply the transformations on it
and save on transformed data path on S3

Usage: This script meant for AWS Glue Job -ETL
with Job Parameters as: 
    --bucket: <bucketname>
    --folder: <folder path of meteostat rawdata>
    --mapped_file: <external file path>
    --region_file: <external file path>

"""

__author__ = "Divesh Chandolia, Sapna Kawate"
__copyright__ = "Copyright 2023, Kearney Sensing Solution"
__version__ = "1.0.1"
__maintainer__ = "Divesh Chandolia, Sapna Kawate"
__email__ = "dchand01@atkearney.com"
__date__ = "March 2023"

# builtin imports 
import logging
import os
from io import StringIO
import sys

# Lib
import pandas as pd
import boto3
from sklearn.preprocessing import normalize

# Platform specific imports
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    'bucket', 
    'folder', 
    'mapped_file', 
    'region_file',
    'crawler_cleaneddata',
    'crawler_transformeddata'
])
# 'MAPPED_WEATHER_STATIONS_file','US_STATE_REGION_file'

# source data
BUCKET = args.get('bucket')
FOLDER = args.get('folder')

# additonal files
MAPPED_WEATHER_STATIONS = args.get('mapped_file')
US_STATE_REGION = args.get('region_file')

# get crawler name
CRAWLER1 = args.get('crawler_cleaneddata')
CRAWLER2 = args.get('crawler_transformeddata')

# Data layers in the S3 bucket
RAW_DIR = 'raw-data'
CLEANED_DIR = 'cleaned-data'
TRANSFORMED_DIR = 'transformed-data'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(BUCKET)

client = boto3.client('s3')
# paginator = client.get_paginator('list_objects_v2')
# result = paginator.paginate(Bucket=BUCKET,Prefix=FOLDER)


def read_csv(file_path, **kwargs):
    "Read data file and return pd dataframe"
    logger.info(f"Reading file: {file_path}")
    try:
        response = client.get_object(Bucket=BUCKET, Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
    except Exception as err:
        logger.error(f"Error while reading: {err}")


def save_csv(df, file_path):
    "Save the DataFrame as CSV in transformed directory"
    try:
        dst_path = file_path.replace(RAW_DIR, TRANSFORMED_DIR)
        logger.info(f"Saving file {dst_path}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving: {err}")


def save_excel(df, file_path):
    "Save the DataFrame as XLSX file in transformed directory"
    try:
        dst_path = file_path.replace(RAW_DIR, TRANSFORMED_DIR)
        dst_path = f"{os.path.splitext(dst_path)[0]}.xlsx"
        dst_path = f"s3://{BUCKET}/{dst_path}"
        logger.info(f"Saving file {dst_path}")
        df.to_excel(dst_path)
    except Exception as err:
        logger.error(f"Error while saving: {err}")

def save_csv_cleaned(df, file_path):
    "Save the DataFrame as CSV in cleaned data dir"
    try:
        dst_path = file_path.replace(RAW_DIR, CLEANED_DIR)
        logger.info(f"Saving file {dst_path}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving: {err}")

def get_folder_list():
    """
    This function returns the list for folders from raw-data which do not exists in transform-data.
    ie. only incremented / newly added directory will be returned
    """
    src_dict = {}
    dst_dict = {}
    SRC_DIR = FOLDER + '/data'
    DST_DIR = SRC_DIR.replace(RAW_DIR, TRANSFORMED_DIR)

    try:
        for objects in bucket.objects.filter(Prefix=SRC_DIR):
            path_str = objects.key
            if path_str.endswith(".csv"):
                dirname = os.path.dirname(path_str)
                try:
                    src_dict[dirname].append(path_str)
                except:
                    src_dict[dirname] = [path_str,]
    except Exception as error:
        logger.error(f"Error: {error}")

    try:
        for objects in bucket.objects.filter(Prefix=DST_DIR):
            path_str = objects.key
            if path_str.endswith(".csv"):
                dirname = os.path.dirname(path_str)
                try:
                    dst_dict[dirname].append(path_str)
                except:
                    dst_dict[dirname] = [path_str,]
    except Exception as error:
        logger.error(f"Error: {error}")

    dst_dict = {k.replace(TRANSFORMED_DIR, RAW_DIR): v for (k, v) in dst_dict.items()}
    return {k: src_dict[k] for k in set(src_dict) - set(dst_dict)}


def apply_transformations(df, file_path):
    """
    It applies transformations on df
    and returned transformed df
    """
    try:
        data = df
        ################################################
        finalweatherst_df = read_csv(MAPPED_WEATHER_STATIONS)
        df_region_state = read_csv(US_STATE_REGION)

        # (xebia) -snow , wdir,wpgt these keys are removed as they are no longer available in above table and giving key error.
        finalweatherdata_df_pivot = data.pivot_table(
            values=['tavg', 'tmin', 'tmax', 'prcp', 'wspd', 'pres', 'tsun'], index=['stationID'], columns=['time'])

        finalweatherdata_df_pivot.reset_index()
        # finalweatherdata_df_pivot1=finalweatherdata_df_pivot.reset_index(level=0, inplace=True)
        finalweatherdata_df_pivot2 = finalweatherdata_df_pivot.T
        finalweatherdata_df_pivot2.to_csv(
            r'Meteostat_temp.csv', index=True, header=True)
        # finalweatherdata_df_pivot2

        # (xebia)- added the 'state' column as it is required in next step i.e. cleaning
        station_region_map = pd.merge(
            finalweatherst_df.loc[:, ["StationID", "region"]],
            df_region_state.loc[:, ["State Code", "Region", "State"]],
            how="left",
            left_on=["region"],
            right_on=["State Code"]
        )
        station_region_map.drop(["region", "State Code"], axis=1, inplace=True)
        station_region_map.rename(
            columns={"StationID": "stationID"}, inplace=True)
        station_region_map = station_region_map.drop_duplicates().reset_index(drop=True)
        # station_region_map
        
        # convert stationID to str
        data['stationID']=data['stationID'].astype(str)
        data = pd.merge(
            data, station_region_map,
            how="left", left_on=["stationID"], right_on=["stationID"]
        )

        # save cleaned data
        save_csv_cleaned(data,file_path)

        data1 = data.groupby(['Region', 'time']).aggregate(
            ['mean', 'min', 'max'])

        data1 = data1.swaplevel(axis=1)
        data1 = data1.reindex(sorted(data1.columns),
                              axis=1).loc[:, ["mean", "min", "max"]]

        df_meteo = pd.read_csv("Meteostat_temp.csv")
        # (xebia)- Renamed the column unnamed:0 to indicator as it required in below code.
        df_meteo.rename(columns={'Unnamed: 0': 'indicator'}, inplace=True)

        column_list = list(df_meteo)
        column_list.remove("indicator")
        column_list.remove("time")

        df_meteo["Avg"] = df_meteo[column_list].mean(axis=1)
        df_meteo["Min"] = df_meteo[column_list].min(axis=1)
        df_meteo["Max"] = df_meteo[column_list].max(axis=1)
        df_meteo[['indicator', 'time', 'Avg', 'Min', 'Max']]

        df_meteo = df_meteo.pivot(
            index='time', columns='indicator', values=['Avg', 'Min', 'Max'])
        df_meteo.columns = ["_".join((i, j)) for i, j in df_meteo.columns]
        df_meteo.index.names = ['Date']
        return df_meteo
        ###############################################
    except Exception as err:
        logger.error(f"Error while transformation: {err}")
        raise Exception(f"while transformation: {err}")
        sys.exit(0)


if __name__ == "__main__":

    logger.info("-- start --")
    folders = get_folder_list()
    if folders:
        for folder, files in folders.items():
            for file_path in files:
                df = read_csv(file_path)
                transformed_df = apply_transformations(df, file_path)
                save_csv(transformed_df, file_path)
                # save_excel(transformed_df,file_path)
        # trigger crawlers
        try:
            logger.info("Triggering Crawlers")
            glue_client = boto3.client('glue')
            glue_client.start_crawler(Name=CRAWLER1)
            glue_client.start_crawler(Name=CRAWLER2)
        except Exception as err:
            logger.error(f"Exception while triggering crawler {err}")    

    else:
        logger.info("No new dir to process")
