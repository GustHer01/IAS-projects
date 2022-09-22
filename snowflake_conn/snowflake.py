import os
import snowflake.connector as sc

class SnowflakeConn():
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
        self.connection = sc.connect(user=SnowflakeConn.user,
                                     host=SnowflakeConn.host,
                                     account=SnowflakeConn.account,
                                     region = SnowflakeConn.region,
                                     password =SnowflakeConn.password,
                                     database=SnowflakeConn.database,      
                                     warehouse=SnowflakeConn.warehouse,  
                                     schema =SnowflakeConn.schema,
                                     authenticator=SnowflakeConn.authenticator)
    def start_connection(self):
        self.cursor = self.connection.cursor()
    
    def _execute_query(self, query):
        result = self.cursor.execute(query).fetchall()
        return result
    
    def close_connection(self):
        self.cursor.close()