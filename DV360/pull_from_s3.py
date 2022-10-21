import os
import boto3
#import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError
#from logs.logger import logger as log

load_dotenv()

def pull_file(filename):
    #logger = log.logger(logging.DEBUG)
    s3 = boto3.client(os.getenv('client'))

    try: 
        s3.download_file(
            Bucket=os.getenv('bucket_name'), Key=os.getenv('key_path'), Filename = filename
        )
        #logger.info('File pulled correctly')
    except ClientError as error:
       #logger.error('File not found, please check your path or your credentials')
       raise error

