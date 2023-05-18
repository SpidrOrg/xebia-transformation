# -*- coding: utf-8 -*-
"""
Short Desc: This programe is a ETL Glue Job for kearney sensing solution

This scripts reads the raw data from source path on S3 bucket
and do the cleaning of files and save in cleaned data path
and apply the transformations on it
and save on transformed data path on S3

It also maintain MNEMONIC_FILE (create & update)"

Usage: This script meant for AWS Glue Job -ETL
with Job Parameters : 
    --bucket: <bucketname>
    --folder: <folder path of ihs rawdata>

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
import dateutil.relativedelta
from datetime import timedelta
import datetime

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
        'crawler_cleaneddata',
        'crawler_transformeddata'
    ])

# source data
BUCKET = args.get('bucket')
FOLDER = args.get('folder')

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

# code specific file path
MNEMONIC_FILE = f"{TRANSFORMED_DIR}/mnemonics/ihs_mnemonics/ihs_mnemonics.csv"

def read_csv(file_path, **kwargs):
    "Read data file and return pd dataframe"
    logger.info(f"Reading file: {file_path}")
    try:
        response = client.get_object(Bucket=BUCKET, Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            logger.debug(
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

def get_folder_dict():
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
        print(f"Error: {error}")

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
        print(f"Error: {error}")

    dst_dict = {k.replace(TRANSFORMED_DIR, RAW_DIR): v for (k, v) in dst_dict.items()}
    return {k: src_dict[k] for k in set(src_dict) - set(dst_dict)}

def read_ihs_mnemonic_file():
    "It read ihs nmemonic file and return dict, if not exists return new empty dict"
    
    try:
        logger.info(f"Reading {MNEMONIC_FILE}")
        df = read_csv(f"{MNEMONIC_FILE}")
        mnemonic_df = df[['mnemonic','description']]
        mnemonic_df = mnemonic_df.set_index('mnemonic')
        return mnemonic_df.to_dict()['description']
    except Exception as err:
        logger.error(f"while read_ihs_mnemonic_file {err} ")
        
    return {}

def apply_transformations(df, file_path):
    "It applied all the transformation rules on df passed to it"
    # get data
    # Drop Columns
    # Set index and sort
    # Remove duplicate columns
    # remove null value columns
    # normalize data
    # Divide Variance into 100 categories 
    # save data

    
    # read existing mnemonic file, 
    # add or update new mnemonic and desc to it
    # and save to same mnemonic file
    mnemonic_df_dict = read_ihs_mnemonic_file()
    columns = ['New Mnemonic','Short Label']
    mnemonic_df = df.reindex(columns=columns)
    # mnemonic_df = df[['New Mnemonic','Short Label']]
    mnemonic_df = mnemonic_df.set_index('New Mnemonic')
    mnemonic_df_dict = {**mnemonic_df_dict, **mnemonic_df.to_dict()['Short Label']}
    mnemonic_df = pd.DataFrame.from_dict(mnemonic_df_dict,orient='index').reset_index()
    mnemonic_df = mnemonic_df.rename(columns={'index':'mnemonic',0:'description'})
    save_csv(mnemonic_df,f"{MNEMONIC_FILE}")

    # maxmonth = MAX_MONTH  # datetime.date(2021, 9, 1)
    try:
        df = df.drop(['Short Label','Mnemonic'], axis=1)
    except:
        df = df

    try:
        df = df.dropna(axis=1,how='all')

        df = df.transpose()
        df = df.rename(columns={'New Mnemonic': 'Date'})
        df.columns = df.iloc[0]
        df = df.iloc[1:,]
        df = df.reset_index().rename(columns={'index': 'Date'})
        df = df.rename(columns={'Month_Starting_Date': 'Date'})

        # Drop columns with duplicate names
        orignum = df.shape[1]
        data = df.loc[:, ~df.columns.duplicated(keep='first')]
        data.isin([np.inf, -np.inf]
                  ).sum()[data.isin([np.inf, -np.inf]).sum() != 0]

        # Take only rows up to maxmonth
        orignum = data.shape[1]
        data['Date'] = pd.to_datetime(data['Date']).apply(lambda x: x.date())

        # save cleaned data
        save_csv_cleaned(data, file_path)

        maxmonth = data['Date'].max()
        logger.info(f"maxmonth:{maxmonth}")
#         maxmonth = datetime.date(2022, 12, 31) 
        
        data = data.loc[data['Date'] < maxmonth, :]
        data.isna().sum()[data.isna().sum() != 0]
        
        data.dropna(axis=1,inplace=True)
        
        if 'pricing and purchasing' in file_path.lower():
            dataset_name = 'IHS Pricing and Purchase'
            logger.info(f"dataset_name:{dataset_name}")
            return data
        
        data_norm = data.drop(['Date'], axis=1)
        
        normalize1 = normalize(data_norm)

        data_scaled = pd.DataFrame(normalize1)
        variance = data_scaled.var()
        var_df = pd.DataFrame({'columns': data_norm.columns.to_list(),
                              'variance': variance})
        var_df['percentile'] = pd.qcut(var_df['variance'].rank(method='first', ascending=False),
                                       100,
                                       labels=False)

        # var_df_fin = var_df.loc[var_df.percentile <= 14]
        var_df_fin = var_df
        var_df_fin.reset_index(drop=True, inplace=True)
        var_df_fin = var_df_fin.iloc[2:]
        final_col = var_df_fin['columns'].to_list()
        data = data[['Date']+final_col]
        return data

    except Exception as e:
        logger.error(f"Exception {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.debug(f"{exc_type}, {fname}, {exc_tb.tb_lineno}")
        logger.debug(f"{exc_type}, {exc_obj}, {exc_tb}")

if __name__ == "__main__":

    logger.info("--Start Transformation--")
    folders = get_folder_dict()
    if folders:
        logger.info(f"folders--{folders}")
        for folder, files in folders.items():
            for file_path in files:
                df = read_csv(file_path)

                transformed_df = apply_transformations(df, file_path)
                if not transformed_df.empty:
                    save_csv(transformed_df, file_path)

        # trigger crawlers
        try:
            glue_client = boto3.client('glue')
            glue_client.start_crawler(Name=CRAWLER1)
            glue_client.start_crawler(Name=CRAWLER2)
        except Exception as err:
            logger.error(f"Exception while triggering crawler {err}")
    else:
        logger.info("No new dir to process")
# transformed_df
