import os
import boto3
from botocore.exceptions import ClientError
from tools import logger as lg
import logging

class athena_connection():
    def __init__(self):
        self.client = boto3.client('athena')
    
    def start_query(self, query_string):
        try:
            response = self.client.start_query_execution(
            QueryString = query_string,
            QueryExecutionContext={
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration={
                'OutputLocation': os.getenv('s3_bucket') ,
            },
            WorkGroup = 'gps-partner'
            )
            return response
        except ClientError as error:
            #logger.error('Query execution start failed')
            raise error
# Get the result of the execution
    def get_execution(self,response):
        logger = lg.logger(logging.DEBUG)
        try:
            get_exec = self.client.get_query_execution(
                QueryExecutionId=response['QueryExecutionId']
            )
    
            status = get_exec['QueryExecution']['Status']['State']
    
            while status != 'SUCCEEDED' and status != 'FAILED':
                get_exec = self.client.get_query_execution(
                    QueryExecutionId=response['QueryExecutionId']
                )
    
                status = get_exec['QueryExecution']['Status']['State']
            if status == 'FAILED':
                logger.error('QUERY FAILED')

            response_results = self.client.get_query_results(
                QueryExecutionId=response['QueryExecutionId']

            )
            return response_results
        except ClientError as error:
            logger.info('Query execution Failed')
            raise error
    
    def get_imps(self, response_results):
        athena_imps = response_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        return athena_imps
    
