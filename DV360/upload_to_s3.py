import os
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from logs.logger import logger as log

load_dotenv()

def upload_file(filename):
    logger = log.logger(logging.DEBUG)
    s3 = boto3.client(os.getenv('client'))

    try:
        s3.upload_file(
            Filename=filename,
            Bucket=os.getenv('bucket_name'),
            Key=os.getenv('key_path')
        )
        logger.info('File uploaded correctly')
    except ClientError as error:
        logger.error('Unable to upload the file, please check path or credentials')
        raise error

