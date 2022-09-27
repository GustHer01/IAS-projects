import os
import snowflake.connector as sc
from dotenv import find_dotenv, load_dotenv

class SnowflakeConn():
    load_dotenv(find_dotenv())
    user=os.getenv('snowflake_user')
    host=os.getenv('snowflake_host')
    account=os.getenv('snowflake_account')
    region = os.getenv('snowflake_region')
    password = os.getenv('snowflake_pass')
    database=os.getenv('snowflake_database')    
    warehouse=os.getenv('snowflake_warehouse')
    schema =os.getenv('snowflake_schema')
    authenticator="externalbrowser"
    
    def __init__(self):
    # class constructor and initiating required data
        self.connection = sc.connect(user=SnowflakeConn.user,
                                     host=SnowflakeConn.host,
                                     account=SnowflakeConn.account,
                                     region = SnowflakeConn.region,
                                     password =SnowflakeConn.password,
                                     database=SnowflakeConn.database,      
                                     warehouse=SnowflakeConn.warehouse,  
                                     schema =SnowflakeConn.schema,
                                     authenticator=SnowflakeConn.authenticator)
    # starts connection to snowflake
    def start_connection(self):
        self.cursor = self.connection.cursor()
    
    # executes query string
    def _execute_query(self, query):
        result = self.cursor.execute(query).fetchall()
        return result
    
    def close_connection(self):
        self.cursor.close()