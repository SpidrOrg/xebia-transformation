# # This scripts reads the raw data from source path on S3 bucket 
# # and apply the configured transformations on it 
# # and save on transformed data path on s3..

# import sys
# import pandas as pd
# import boto3
# import io
# import logging
# from awsglue.utils import getResolvedOptions

# # Logger
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# handler = logging.StreamHandler(sys.stdout)
# formatter = logging.Formatter(
#     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

# args = getResolvedOptions(sys.argv,['bucket','prefix','filepath','crawler_cleaneddata',
#         'crawler_transformeddata'])

# bucket = args.get('bucket')
# prefix = args.get('prefix')
# filepath = args.get('filepath')

# # get crawler name
# CRAWLER1 = args.get('crawler_cleaneddata')
# CRAWLER2 = args.get('crawler_transformeddata')

# # Data layers in the S3 bucket
# RAW_DIR = 'raw-data'
# CLEANED_DIR = 'cleaned-data'
# TRANSFORMED_DIR = 'transformed-data'

# s3_client = boto3.client("s3")

# dataframes = []

# def clean_file():
#     #Fetching the data files from s3 bucket
#     try:
#         response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

#         logger.info("Getting the list of objects in S3 bucket.")
#         objs = response.get("Contents")
    
#         l=[]
#         logger.info("appending the S3 file paths for transformation in a list.")
#         for i in objs:
#             if i['Key'].endswith('.csv'):
#                 l.append(i['Key'])
            
#         logger.info("This function transforms the google_trends data")
        
#          #Cleaning the ingested files.
#          #renaming the columns, dropping duplicates,changing the date format.
#         for i in l:
#             obj = s3_client.get_object(Bucket=bucket , Key=i)
#             tempdf = pd.read_csv(obj['Body'])
            
#             logger.info("Renaming the columns..")
#             tempdf3 = tempdf.rename(columns= {'google_trends':'Google_Trend_Interest_over_time_web','google_trends_web':'Google_Trend_Interest_over_time_image','youtube':'Google_Trend_Interest_over_time_youtube'})
            
#             logger.info("Converting date format to 'yyyy-mm-dd'.")
#             tempdf3['Date']=pd.to_datetime(tempdf3['week_start_date']).dt.to_period('M')
#             tempdf3['Date']= tempdf3['Date'].astype(str)+'-01'
#             tempdf3['Date']= pd.to_datetime(tempdf3['Date'])
#             tempdf3['Date'] = tempdf3['Date'].dt.strftime('%Y-%m-%d')
#             dataframes.append(tempdf3)
            
#         logger.info("Concating the datafarmes.")
#         df = pd.concat(dataframes)
        
#         logger.info("Dropping duplicates and unwanted columns.")
#         df1=df.drop_duplicates()
#         df2=df1.drop(['Unnamed: 0'],axis=1)
        
#         logger.info("Writing the transformed data into a csv.")
#         csv_buffer=io.StringIO()
#         filename = filepath + 'google_trends' + '.csv'
#         df2.to_csv(csv_buffer,index=False)
#         content = csv_buffer.getvalue()
#         s3_client.put_object(Bucket=bucket, Body=content,Key=filename)
       
#         return df2
        
#     except Exception as e:
#         logger.info(e)
        
# def transform():
#     tempdf = clean_file()
    
#     #Transforming the clean file 
#     try:
#         logger.info("Grouping the data by date and category to find the total searches over the month.")
#         df=tempdf.groupby(['Date','Category']).sum().reset_index()
        
#         csv_buffer=io.StringIO()
#         filename = filepath.replace(CLEANED_DIR,TRANSFORMED_DIR) + 'google_trends' + '.csv'
#         df.to_csv(csv_buffer,index=False)
#         content = csv_buffer.getvalue()
#         s3_client.put_object(Bucket=bucket, Body=content,Key=filename)
        
#         # trigger crawlers
#         try:
#             logger.info("Triggering Crawler")
#             glue_client = boto3.client('glue')
#             glue_client.start_crawler(Name=CRAWLER1)
#             glue_client.start_crawler(Name=CRAWLER2)
#         except Exception as err:
#             logger.error(f"Exception while triggering crawler {err}")
    
#         return df
        
#     except Exception as e:
#         logger.info(e)

      
# if __name__=="__main__":
        
#     logger.info("--Start Transformation--")
    
#     #calling the transform function to carry out the transformation
    
#     transform()