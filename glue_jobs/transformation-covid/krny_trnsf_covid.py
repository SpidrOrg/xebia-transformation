# -*- coding: utf-8 -*-
"""
Short Desc: This programe is a ETL Glue Job for kearney sensing solution

This scripts reads the raw data from source path on S3 bucket
and do the merging and cleaning of files and save in cleaned data path
and apply the transformations on it
and save on transformed data path on S3

Usage : This script meant for AWS Glue Job -ETL
with Job Parameters as :
    --bucket: <bucketname>
    --folder: <folder path of covid rawdata>
    --irm_file: <external file path>
    --crawler_cleaneddata: <crawler name for cleaned data>
    --crawler_transformeddata: <crawler name for tarnsformed data>

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

# Platform specific imports
from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, [
    'bucket', 
    'folder', 
    'irm_file',
    'crawler_cleaneddata',
    'crawler_transformeddata'
])

# source data
BUCKET = args.get('bucket')
FOLDER = args.get('folder')

# get crawler name
CRAWLER1 = args.get('crawler_cleaneddata')
CRAWLER2 = args.get('crawler_transformeddata')

# additional files
IRM_FILE_PATH = args.get('irm_file')

# Code specific field names
COUNTRY = 'US'
rubbish_states = ['Department of Defense',
                  'Federal Bureau of Prisons',
                  'Indian Health Services',
                  'Long Term Care (LTC) Program',
                  'Veterans Health Administration'
                  ]
REVELANT_COLS = ['Province_State', 'Date',
                 'People_at_least_one_dose', 'People_fully_vaccinated']

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
        raise Exception(f"While reading file: {err}")


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
        
def save_csv_raw(df, file_path):
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


def apply_transformations(folder, files):
    """
    It reads vaccine and covidcases files 
    clean and merge them and apply transformations
    merge with external file for state and population info
    """
    try:
        logger.debug(folder, files)
        for file_path in files:
            if 'vaccinedata' in file_path:
                try:
                    # Get vaccine data
                    vaccine_df = read_csv(file_path)
                    # Select US only
                    vaccine_df = vaccine_df.loc[vaccine_df['Country_Region'] == COUNTRY, :].reset_index(
                    )
                    # Remove rubbish states
                    vaccine_df = vaccine_df.loc[~(
                        vaccine_df['Province_State'].isin(rubbish_states)), :]
                    # Format date
                    vaccine_df['Date'] = pd.to_datetime(vaccine_df['Date'])
                    vaccine_df['Date'] = vaccine_df['Date'].apply(
                        lambda x: x.date())
                    # Sort table
                    vaccine_df = vaccine_df.sort_values(
                        ['Province_State', 'Date'])
                    # Select only relevant cols
                    vaccine_df = vaccine_df[REVELANT_COLS]
                    # Calculate partial vaccinations
                    vaccine_df['people_partially_vaccinated'] = vaccine_df['People_at_least_one_dose'].\
                        fillna(0) - \
                        vaccine_df['People_fully_vaccinated'].fillna(0)
                    # Rename columns
                    vaccine_df = vaccine_df.rename(columns={'People_fully_vaccinated': 'people_fully_vaccinated',
                                                            'People_at_least_one_dose': 'total_vaccinations'})
                except Exception as err:
                    logger.error(f"Error while transforming: {err}")
                    logger.error(f"file_path: {file_path}")
                    sys.exit(0)

            elif 'covidcases' in file_path:

                try:
                    # read data file as df
                    cases_df = read_csv(file_path)
                    # Select US only
                    cases_df = cases_df.loc[cases_df['Country_Region'] == 'US', :].reset_index(
                    )
                    # Format date
                    cases_df['Date'] = pd.to_datetime(
                        cases_df['Date'], format="%Y-%m-%d")

                    cases_df['Date'] = cases_df['Date'].apply(
                        lambda x: x.date())
                    # Sort table
                    cases_df = cases_df.sort_values(['Province_State', 'Date'])
                    # Select only relevant cols
                    cases_df = cases_df[['Province_State',
                                         'Date', 'Confirmed', 'Deaths']]

                    # Calculate 7 Day Average New Cases
                    cases_df['New Cases'] = cases_df.loc[:, 'Confirmed'] - \
                        cases_df.loc[:, 'Confirmed'].shift(1).fillna(0)
                    cases_df.loc[cases_df['New Cases'] < 0] = 0
                    # Remove new cases for 1st months
                    for p in cases_df['Province_State'].unique():
                        mindate = cases_df.loc[cases_df['Province_State'] == p, 'Date'].min(
                        )
                        cases_df.loc[(cases_df['Province_State'] == p) & (
                            cases_df['Date'] == mindate), 'New Cases'] = 0
                    cases_df['7 Day Average New Cases'] = cases_df.loc[:,
                                                                       'New Cases'].rolling(window=7).mean()
                    # Rename columns
                    cases_df = cases_df.rename(
                        columns={'Confirmed': 'total_cases', 'Deaths': 'total_deaths'})
                    # Get rid of rubbish states
                    rubbish_states = ['Recovered', 0]
                    cases_df = cases_df.loc[~(
                        cases_df['Province_State'].isin(rubbish_states)), :]
                    cases_df = cases_df.loc[cases_df['Province_State'] != 0]
                    # cases_df.to_csv(dest_path,index=False)
                except Exception as err:
                    logger.error(f"Error while transforming: {err}")
                    logger.error(f"file_path: {file_path}")
                    sys.exit(0)
            else:
                logger.info(f"No case found for {file_path}")

        # Operations on third file
        try:
            pop = read_csv(IRM_FILE_PATH)  # irm_data
            options = ['UNITED STATES',]
            pop = pop[pop['Country'].isin(options)]
            pop = pop.loc[pop['Province_State_'] != 'z_total']
            pop = pop.loc[~(pop['Population'].isna()), :]
            pop = pop.rename(columns={'Province_State_': 'Province_State'})
            irm = pop[['Date', 'Province_State', 'Inverse Risk Metric']]
            pop = pop[['Province_State', 'Population']
                      ].drop_duplicates('Province_State')
        except Exception as err:
            logger.error(f"Error while reading IRM file: {err}")

        # Merge data
        if (not vaccine_df.empty and not cases_df.empty):
            covid_df = pd.merge(vaccine_df, cases_df, left_on=['Date', 'Province_State'],
                                right_on=['Date', 'Province_State'], how='outer')

            del vaccine_df
            del cases_df

            # Merge population
            covid_df['Province_State'] = covid_df['Province_State'].apply(
                lambda x: str(x).upper())
            covid_df = pd.merge(covid_df, pop, on='Province_State', how='left')
            # Drop minor states without population stats
            origstates = set(covid_df['Province_State'])
            covid_df = covid_df.loc[~(covid_df['Population'].isna()), :]
            # print(f"Dropped minor states due to missing population stats: {origstates-set(covid_df['Province_State'])}")

            # Merge IRM
            irm['Date'] = pd.to_datetime(irm['Date']).apply(lambda x: x.date())
            covid_df = pd.merge(covid_df, irm, on=[
                                'Province_State', 'Date'], how='left')

            # Fill down missing IRM per state (recent months)
            covid_df = covid_df.sort_values(['Province_State', 'Date'])
            covid_df['Inverse Risk Metric'] = covid_df['Inverse Risk Metric'].ffill()

            # Saving as merged and clean data
            dst_file = f"{folder}/covid.csv"
            save_csv_raw(covid_df, dst_file)

        ##############
        if not covid_df.empty:
            # Calculate population dependent features
            covid_df['total_vaccinations_per_hundred'] = (
                covid_df['total_vaccinations']/covid_df['Population'])*100
            covid_df['people_vaccinated_per_hundred'] = (
                covid_df['people_partially_vaccinated']/covid_df['Population'])*100
            # covid_df
            covid_df['Province_State'] = covid_df['Province_State'].apply(
                lambda x: str(x).upper())
            # Take only rows up to maxmonth
            covid_df['month_year'] = pd.to_datetime(
                covid_df['Date']).apply(lambda x: x.replace(day=1).date())

            # Calculate monthly per state
            covid_df_monthly_state = covid_df.groupby(['month_year', 'Province_State']).agg({
                'people_fully_vaccinated': 'last',
                'people_partially_vaccinated': 'last',
                # 'Inverse Risk Metric':'last',
                'Population':'sum',
                'total_cases': 'last',
                'total_deaths': 'last',
                'total_vaccinations': 'last',
                'total_vaccinations_per_hundred':'last',
                'people_vaccinated_per_hundred':'last',
                '7 Day Average New Cases': 'last'}).reset_index()

            # Calculate monthly 
            covid_df_monthly = covid_df_monthly_state.groupby(['month_year']).agg({'people_fully_vaccinated': 'sum',
                'people_partially_vaccinated': 'sum',
                # 'Inverse Risk Metric':'mean',
                'Population':'sum',
                'total_cases': 'sum',
                'total_deaths': 'sum',
                'total_vaccinations': 'sum',
                'total_vaccinations_per_hundred':'mean',
                'people_vaccinated_per_hundred':'mean',
                '7 Day Average New Cases': 'sum'}).reset_index().rename(columns={'month_year': 'Date'})
            
            # Saving at destination
            covid_df_monthly_state.rename(columns={'month_year': 'Date'},inplace=True)
            # dst_file = f"{folder}/covid_monthly_state.csv"
            # save_csv(covid_df_monthly_state, dst_file)
            dst_file = f"{folder}/covid_monthly.csv"
            save_csv(covid_df_monthly, dst_file)

    except Exception as err:
        logger.error(f"Error while transformation: {err}")
        raise Exception(f"While transformation: {err}")


if __name__ == "__main__":
    logger.info("-- start --")
    folders = get_folder_list()
    if folders:
        for folder, files in folders.items():
            apply_transformations(folder, files)

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
