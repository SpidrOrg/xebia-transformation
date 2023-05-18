# -*- coding: utf-8 -*-
"""
Short Desc: This programe is a ETL Glue Job for kearney sensing solution

This scripts reads the raw data from source path on S3 bucket
and do the cleaning of files and save in cleaned data path
and apply the transformations on it
and save on transformed data path on S3

Usage: This script meant for AWS Glue Job -ETL
with Job Parameters as: 
    --bucket: <bucketname>
    --folder: <folder path of yahoo_finance rawdata>
    --table_name: <dynamodb table name of yahoo securites with col ticker and ticeker_name>

"""

__author__ = "Divesh Chandolia"
__copyright__ = "Copyright 2023, Kearney Sensing Solution"
__version__ = "1.0.1"
__maintainer__ = "Divesh Chandolia"
__email__ = "dchand01@atkearney.com"
__date__ = "March 2023"

# builtin imports 
import logging
import os
from io import StringIO
import sys

# Lib
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
import boto3

# Platform specific imports
from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, [
    'bucket', 
    'folder',
    'table_name',
    'crawler_cleaneddata',
    'crawler_transformeddata'
])

# source data
BUCKET = args['bucket']
FOLDER = args['folder']
MAPPER_TABLE = args['table_name']

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


def get_mapper():
    "It retrives ticker and ticker_name from dynamodb table for mapping column name"
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(MAPPER_TABLE)
        response = table.scan()
        data = response['Items']

        return {item['ticker']: item['ticker_name'] for item in data}
    except Exception as err:
        logger.error(f"Error while reading mapper: {err}")
        raise Exception(f"Exception raised: {err}")


def read_csv(file_path, **kwargs):
    "Read data file and return pd dataframe"
    logger.info(f"Reading file: {file_path}")
    try:
        response = client.get_object(Bucket=BUCKET, Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            logger.info(
                f"Successful S3 get_object response. Status - {status}")
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
    #     def get_folder_dict():
    """
    This function returns the list for folders from raw-data which do not exists in transform-data.
    ie. only incremented / newly added directory will be returned
    """
    src_dict = {}
    dst_dict = {}
    SRC_DIR = FOLDER
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


def apply_transformations(df, mapper_dict, file_path):
    """
    It applies transformations on df
    and remane the columns as per mapper_dict
    """
    try:
        # Calculate average of Open & Close
        df['score'] = df[['open', 'close']].mean(axis=1)
        df = df[['Date', 'colname', 'score']]

        # df.colname.value_counts()
        df = df.pivot(index='Date', columns='colname').reset_index()

        # Clean column names
        df.columns = df.columns.map(''.join)
        df.columns = [x.replace('score', '') for x in df.columns]

        # Rename columns based on mapper
        df = df.rename(columns=mapper_dict)

        # Convert the date column to a datetime object
        df['Date'] = pd.to_datetime(df['Date'])
        
        # save cleaned data
        save_csv_cleaned(df, file_path)

        # Group the data by month, and get the data on the first day of each month
        df = df.groupby(pd.Grouper(key='Date', freq='MS')).first()

        # Forward fill
        # df_monthly.ffill(0, inplace=True)
        return df
    except Exception as err:
        logger.error(f"Error while transformation: {err}")
        raise Exception(f"Exception raised: {err}")


if __name__ == "__main__":
    logger.info("-- start --")
    folders = get_folder_list()
    if folders:
        mapper_dict = get_mapper()
        logger.debug(f"folders--{folders}")
        logger.debug(f"mapper_dict--{mapper_dict}")
        for folder, files in folders.items():
            for file_path in files:
                df = read_csv(file_path)
                transformed_df = apply_transformations(
                    df, mapper_dict, file_path)
                save_csv(transformed_df, file_path)
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
