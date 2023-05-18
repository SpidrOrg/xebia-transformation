# # -*- coding: utf-8 -*-
# """
# Short Desc: This programe is a ETL Glue Job for kearney sensing solution

# This scripts reads the raw data from source path on S3 bucket
# and do the cleaning of files and save in cleaned data path
# and apply the transformations on it
# and save on transformed data path on S3

# Usage: This script meant for AWS Glue Job -ETL
# with Job Parameters as: 
#     --bucket: <bucketname>
#     --folder: <folder path of similarweb rawdata>

# """

# __author__ = "Divesh Chandolia"
# __copyright__ = "Copyright 2023, Kearney Sensing Solution"
# __version__ = "1.0.1"
# __maintainer__ = "Divesh Chandolia"
# __email__ = "dchand01@atkearney.com"
# __date__ = "March 2023"

# # builtin imports 
# import logging
# import os
# from io import StringIO
# import sys
# from functools import reduce

# # Lib
# import pandas as pd
# from sklearn.preprocessing import normalize
# import boto3

# # Platform specific imports
# from awsglue.utils import getResolvedOptions
# args = getResolvedOptions(sys.argv, [
#     'bucket', 
#     'folder',
#     'crawler_cleaneddata',
#     'crawler_transformeddata'
# ])


# # Data layers in the S3 bucket
# RAW_DIR = 'raw-data'
# CLEANED_DIR = 'cleaned-data'
# TRANSFORMED_DIR = 'transformed-data'


# # source data
# BUCKET = args.get('bucket')
# FOLDER = args.get('folder')

# # get crawler name
# CRAWLER1 = args.get('crawler_cleaneddata')
# CRAWLER2 = args.get('crawler_transformeddata')

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# handler = logging.StreamHandler(sys.stdout)
# formatter = logging.Formatter(
#     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

# s3_resource = boto3.resource('s3')

# bucket = s3_resource.Bucket(BUCKET)

# client = boto3.client('s3')

# # paginator = client.get_paginator('list_objects_v2')
# # result = paginator.paginate(Bucket=BUCKET,Prefix=FOLDER)


# def read_csv(file_path, **kwargs):
#     "Read data file and return pd dataframe"
#     logger.info(f"Reading file: {file_path}")
#     try:
#         response = client.get_object(Bucket=BUCKET, Key=file_path)
#         status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
#         if status == 200:
#             print(f"Successful S3 get_object response. Status - {status}")
#             return pd.read_csv(response.get("Body"))
#     except Exception as err:
#         logger.error(f"Error while reading: {err}")


# def save_csv(df, file_path):
#     "Save the DataFrame as CSV in transformed directory"
#     try:
#         dst_path = file_path.replace(RAW_DIR, TRANSFORMED_DIR)
#         logger.info(f"Saving file {dst_path}")
#         csv_buffer = StringIO()
#         df.to_csv(csv_buffer, index=False)
#         s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
#     except Exception as err:
#         logger.error(f"Error while saving: {err}")

# def save_csv_cleaned(df, file_path):
#     "Save the DataFrame as CSV in cleaned data dir"
#     try:
#         dst_path = file_path.replace(RAW_DIR, CLEANED_DIR)
#         logger.info(f"Saving file {dst_path}")
#         csv_buffer = StringIO()
#         df.to_csv(csv_buffer, index=False)
#         s3_resource.Object(BUCKET, dst_path).put(Body=csv_buffer.getvalue())
#     except Exception as err:
#         logger.error(f"Error while saving: {err}")

# def get_folder_list():
#     """
#     This function returns the list for folders from raw-data which do not exists in transform-data.
#     ie. only incremented / newly added directory will be returned
#     """
#     src_dict = {}
#     dst_dict = {}
#     SRC_DIR = FOLDER + '/data'
#     DST_DIR = SRC_DIR.replace(RAW_DIR, TRANSFORMED_DIR)

#     try:
#         for objects in bucket.objects.filter(Prefix=SRC_DIR):
#             path_str = objects.key
#             if path_str.endswith(".csv"):
#                 dirname = os.path.dirname(path_str)
#                 try:
#                     src_dict[dirname].append(path_str)
#                 except:
#                     src_dict[dirname] = [path_str,]
#     except Exception as error:
#         logger.error(f"Error: {error}")

#     try:
#         for objects in bucket.objects.filter(Prefix=DST_DIR):
#             path_str = objects.key
#             if path_str.endswith(".csv"):
#                 dirname = os.path.dirname(path_str)
#                 try:
#                     dst_dict[dirname].append(path_str)
#                 except:
#                     dst_dict[dirname] = [path_str,]
#     except Exception as error:
#         logger.error(f"Error: {error}")

#     dst_dict = {k.replace(TRANSFORMED_DIR, RAW_DIR): v for (k, v) in dst_dict.items()}
#     return {k: src_dict[k] for k in set(src_dict) - set(dst_dict)}


# def apply_transformations(folder, files):
#     """
#     It reads data files and apply transformations on it 
#     """
#     try:
#         print(folder, files)
#         for file_path in files:
#             if 'conversion_dashboard' in file_path:

#                 # Read first SimilarWeb file
#                 df1 = read_csv(file_path)

#                 # Remove amazon.com without segment
#                 # df1 = df1.loc[~((df1['Domains'] == 'amazon.com')
#                 #                 & (df1['Segment'].isna())), :]

#                 # Remove Group Average
#                 df1=df1.loc[df1['Domains']!='Group Average']
#                 # Drop duplicates, keeping new extract
#                 df1 = df1.drop_duplicates(
#                     ['Domains', 'Time Period'], keep='first')
#                 # Add Segment to Amazon domain
#                 df1.loc[df1['Domains'] == 'amazon.com', 'Domains'] = df1.loc[df1['Domains'] == 'amazon.com', 'Domains']\
#                     + '-' + df1.loc[df1['Domains'] =='amazon.com', 'Segment'].fillna('')
#                 # save cleaned data
#                 save_csv_cleaned(df1,file_path)

#                 del df1['Segment']
#                 df1['Domains'].value_counts()
#                 df1 = df1.pivot(index='Time Period',columns='Domains').reset_index()
#                 df1.columns = df1.columns.map('_'.join)
#                 df1.columns = ['SW_' + x for x in df1.columns]
#                 df1 = df1.rename(columns={'SW_Time Period_': 'Time Period'})

#             elif 'totaltraffic_sources' in file_path:
#                 # Read second SimilarWeb file (for online traffic)
#                 df2_new = read_csv(file_path)
#                 # df2_new['Domain'].value_counts()
#                 # Drop duplicates, keeping new extract
#                 df2 = df2_new
#                 df2 = df2.drop_duplicates(
#                     ['Domain', 'Time Period', 'Channel Traffic'], keep='first')
#                 # Append both files
#                 # df2 = df2_new.append(df1, ignore_index=True)
#                 # Drop duplicates, keeping new extract
#                 df2 = df2.drop_duplicates(
#                     ['Domain', 'Time Period', 'Channel Traffic'], keep='first')
#                 df2['Domain'].value_counts()

#                 # Replace string <5000 with constant 5000
#                 df2.loc[df2['Channel Traffic'] ==
#                         '<5,000.00', 'Channel Traffic'] = 2500
#                 df2['Channel Traffic'] = df2['Channel Traffic'].astype('float')

#                 # save cleaned data
#                 save_csv_cleaned(df2,file_path)

#                 # Calculate monthly ol traffic per domain
#                 df2 = df2.groupby(['Time Period', 'Domain']).agg(
#                     {'Channel Traffic': 'sum'}).reset_index()
#                 # Pivot
#                 df2 = df2.pivot(index='Time Period',
#                                 columns='Domain').reset_index()
#                 df2.columns = df2.columns.map(''.join)
#                 df2.columns = [x.replace('Channel Traffic', '')
#                                for x in df2.columns]

#                 # Rename columns based on variable tracker
#                 df2 = df2.rename(columns={'amazon.com': 'SW_amazon_ol_Traffic',
#                                           'homedepot.com': 'SW_homedepot_ol_traffic',
#                                           'lowes.com': 'SW_Lowes_ol_traffic',
#                                           'truevalue.com': 'SW_truevalue_OL_traffic'})

#             else:
#                 print(f"No case found for {file_path}")

#         # Merge both datasets
#         df_merged = pd.merge(df1, df2, on='Time Period', how='left')
#         df_merged= df_merged.rename(columns={'Time Period':'Date'})
#         return df_merged

#     except Exception as err:
#         logger.error(f"Error while transformation: {err}")
#         raise Exception(f"Exception while transformation {err}")
#         sys.exit(0)


# if __name__ == "__main__":
#     logger.info("-- start --")
#     folders = get_folder_list()
#     if folders:
#         for folder, files in folders.items():
#             if len(files) < 2:
#                 logger.error(f"Both files are not available in directory; {files}")
#                 continue
#             transformed_df = apply_transformations(folder, files)
#             if not transformed_df.empty:
#                 save_csv(transformed_df, f"{folder}/similarweb_clean.csv")

#         # trigger crawlers
#         try:
#             glue_client = boto3.client('glue')
#             glue_client.start_crawler(Name=CRAWLER1)
#             glue_client.start_crawler(Name=CRAWLER2)
#         except Exception as err:
#             logger.error(f"Exception while triggering crawler {err}")
#     else:
#         logger.info("No new dir to process")
