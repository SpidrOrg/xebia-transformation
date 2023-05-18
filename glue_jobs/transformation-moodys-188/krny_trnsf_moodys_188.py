# -*- coding: utf-8 -*-
"""
Short Desc: This programe is a ETL Glue Job for kearney sensing solution

This scripts reads the raw data from source path on S3 bucket
and do the cleaning of files and save in cleaned data path
and apply the transformations on it
and save on transformed data path on S3..3

Usage: This script meant for AWS Glue Job -ETL
with Job Parameters as: 
    --bucket: <bucketname>
    --folder: <folder path of moodys rawdata>

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
from io import StringIO, BytesIO
import sys
from functools import reduce

# Lib
import pandas as pd
import boto3

# Platform specific imports
from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, [
    'bucket', 
    'folder',
    'config',
    'crawler_cleaneddata',
    'crawler_transformeddata'
])

# Data layers in the S3 bucket
RAW_DIR = 'raw-data'
CLEANED_DIR = 'cleaned-data'
TRANSFORMED_DIR = 'transformed-data'

# source data
BUCKET = args.get('bucket')
FOLDER = args.get('folder')
CONFIG = args.get('config')

# get crawler name
CRAWLER1 = args.get('crawler_cleaneddata')
CRAWLER2 = args.get('crawler_transformeddata')

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

def save_parquet(df, file_path):
    "Save the DataFrame as PARQUET in cleaned-data directory"
    try:
        dst_path = file_path.replace(RAW_DIR, TRANSFORMED_DIR)
        pq_buffer = BytesIO()
        dst_path = os.path.splitext(dst_path)[0]+'.parquet'
        logger.info(f"Saving file {dst_path}")
        df.to_parquet(pq_buffer, index=False)
        # s3_client.put_object(Bucket=BUCKET, Key=dst_path, Body=pq_buffer.getvalue())
        s3_resource.Object(BUCKET, dst_path).put(Body=pq_buffer.getvalue())
    except Exception as err:
        logger.error(f"Error while saving: {err}")

def save_parquet_cleaned(df, file_path):
    "Save the DataFrame as PARQUET in cleaned-data directory"
    try:
        dst_path = file_path.replace(RAW_DIR, CLEANED_DIR)
        pq_buffer = BytesIO()
        dst_path = os.path.splitext(dst_path)[0]+'.parquet'
        logger.info(f"Saving file {dst_path}")
        df.to_parquet(pq_buffer, index=False)
        # s3_client.put_object(Bucket=BUCKET, Key=dst_path, Body=pq_buffer.getvalue())
        s3_resource.Object(BUCKET, dst_path).put(Body=pq_buffer.getvalue())
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
            if path_str.endswith(".parquet"):
                dirname = os.path.dirname(path_str)
                try:
                    dst_dict[dirname].append(path_str)
                except:
                    dst_dict[dirname] = [path_str,]
    except Exception as error:
        logger.error(f"Error: {error}")

    dst_dict = {k.replace(TRANSFORMED_DIR, RAW_DIR): v for (k, v) in dst_dict.items()}
    return {k: src_dict[k] for k in set(src_dict) - set(dst_dict)}


def apply_transformations(df,file_path):
    """
    It applies transformations on df
    and returned transformed df
    """
    try:
        # df['Date'] = (list(map(lambda x: datetime.strptime(
        #     x, "%Y-%m-%d").strftime("%Y-%m-%d"), df['Date'])))

        # convert `2010Q1 type date format to date string
        try:
            df.date.replace(to_replace ='Q1', value ="-03-31" , regex = True, inplace = True)
            df.date.replace(to_replace ='Q2', value ="-06-30" , regex = True, inplace = True)
            df.date.replace(to_replace ='Q3', value ="-09-30" , regex = True, inplace = True)
            df.date.replace(to_replace ='Q4', value ="-12-31" , regex = True, inplace = True)
        except Exception as err:
            logger.error(f"Exception while date string conversion: {err}")
        # rename date to Date
        df.rename(columns={'date':'Date'},inplace = True)

        df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
        # cleaned data save
        # save_csv_cleaned(df, file_path)
        save_parquet_cleaned(df, file_path)

        # Format date
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].apply(lambda x: x.replace(day=1))

        # ordering
        return df.sort_values(by=['Date'], ascending=True)

    except Exception as e:
        logger.error(f"Error while transformation: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.debug(f"{exc_type}, {fname}, {exc_tb.tb_lineno}")
        logger.debug(f"{exc_type}, {exc_obj}, {exc_tb}")
    # except Exception as err:
    #     logger.error(f"Error while transformation: {err}")

if __name__ == "__main__":
    logger.info("-- start --")
    folders = get_folder_list()
    if folders:
        for folder, files in folders.items():
            for file_path in files:
                logger.debug(file_path)
                df = read_csv(file_path)
                transformed_df = apply_transformations(df,file_path)
                if not transformed_df.empty:
                    # save_csv(transformed_df, file_path)
                    save_parquet(transformed_df, file_path)

        # Update mnemonics file from raw to transformed data
        try:
            MNEMONIC_FILE = f"{TRANSFORMED_DIR}/mnemonics/moodys_188_mnemonics/moodys_188_mnemonics.csv"
            logger.info(f"Updating mnemonics file: {MNEMONIC_FILE}")
            logger.info( f"{CONFIG}/moodys_188_mnemonics.csv")
            copy_source = {
                'Bucket': f"{BUCKET}",
                'Key': f"{CONFIG}/moodys_188_mnemonics.csv"
            }
            s3_resource.meta.client.copy(copy_source, f"{BUCKET}", MNEMONIC_FILE)
        except Exception as err:
            logger.error(f"Exception while mnenomics file {err}")

        # trigger crawlers
        try:
            logger.info(f"Triggering Crawlers {CRAWLER1},{CRAWLER2}")
            glue_client = boto3.client('glue')
            glue_client.start_crawler(Name=CRAWLER1)
            glue_client.start_crawler(Name=CRAWLER2)
        except Exception as err:
            logger.error(f"Exception while triggering crawler {err}")
    else:
        logger.info("No new dir to process")

