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
    --folder: <folder path of fred rawdata>
    --mapper: <dynamodb table name of fred>
    --crawler_cleaneddata: <crawler name for cleaned data>
    --crawler_transformeddata: <crawler name for tarnsformed data>

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
import io
import sys
from functools import reduce

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
        'mapper',
        'crawler_cleaneddata',
        'crawler_transformeddata'
    ])

# Source data
BUCKET = args['bucket']
FOLDER = args['folder']
MAPPER_TABLE = args['mapper']

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

def get_mapper():
    "It retrives Series_ID and Series_Name from dynamodb table for mapping column name"
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(MAPPER_TABLE)
        response = table.scan()
        data = response['Items']

        return {item['Series_ID']: item['Series_Name'] for item in data}
    except Exception as err:
        logger.error(f"Error while reading mapper: {err}")
        sys.exit(0)

def read_csv(file_path, **kwargs):
    "Read csv data file and return pd dataframe"
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
        df.to_csv(csv_buffer, index=False)
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


def apply_transformations(folder, files, mapper_dict):
    """
    It creates list of df from list of files
    merged it and apply transformations 
    """
    try:
        logger.debug('files--', files)
        dfs = [read_csv(file) for file in files]
        df_merged = reduce(lambda left, right: pd.merge(
            left, right, on=['DATE'], how='outer'), dfs)

        # save cleaned data
        file_path = f"{folder}/fred.csv"
        save_csv_cleaned(df_merged, file_path)
        return df_merged.rename(columns=mapper_dict)
    except Exception as err:
        logger.error(f"Error while transformation: {err}")
        raise Exception(f"while transformation: {err}")


if __name__ == "__main__":
    logger.info("-- start --")
    folders = get_folder_list()
    mapper_dict = get_mapper()
    logger.debug(mapper_dict)
    logger.debug(folders)
    if folders and mapper_dict:
        for folder, files in folders.items():
            transformed_df = apply_transformations(folder, files, mapper_dict)
            if not transformed_df.empty:
                save_csv(transformed_df, f"{folder}/fred.csv")

        # trigger crawlers
        try:
            logger.info("Triggering crawlers")
            glue_client = boto3.client('glue')
            glue_client.start_crawler(Name=CRAWLER1)
            glue_client.start_crawler(Name=CRAWLER2)
        except Exception as err:
            logger.error(f"Exception while triggering crawler {err}")
    else:
        logger.info("No new dir to process")
