#!/usr/bin/env python
# coding: utf-8

import os
import boto3
import pygsheets
import pandas as pd
from alerts.email_test import email_alert
from snowflake_conn.snowflake import SnowflakeConn
from datetime import date, timedelta

# Given a df with the impressions from athena and snowflake check for discreps between the sources
def discreps_alert(partners, alert_obj):
    discreps = []
    for x in range(len(partners)):
        if int(partners.difference.loc[x]) > 10 or int(partners.difference.loc[x]) < -10:
            discreps.append(partners.partner.loc[x])
    if discreps:
        alert_obj.send_email(f"""Date: {partners.date.loc[1]}\nDiscrep > than 10% detected for: {discreps}\nPlease check: Please check: {os.getenv('googlesheet_url')} for details""")
    return 

# Google sheet to paste impressions and calculations
class GoogleSheets():
    file = './client_secret.json'
    url = os.getenv('googlesheet_url')
    sheet_number = 0
    
    def __init__(self):
        self.gc = pygsheets.authorize(service_account_file=GoogleSheets.file)
        self.sh = self.gc.open_by_url(GoogleSheets.url)

    def set_worksheet(self):
        self.wk = self.sh[int(GoogleSheets.sheet_number)]
        return self.wk

# Get the actual date to use it in athena and snowflake
# Formating actual date to athenas format
def get_date():
    today = date.today()
    yesterday = today - timedelta(days = 1)
    dates = [yesterday, today]
    format_dates = []
    
    for x in dates:
        d = str(x)
        d = d.split('-')
        
        tmp = d[0]
        for y in range(1,len(d)):
            tmp = tmp + str(d[y])
        
        format_dates.append(tmp)

    return format_dates

# Setting the service to use for boto3 library
def set_client():
    a_client = boto3.client('athena')
    return a_client

def start_query(client, query_String):
    response = client.start_query_execution(
    QueryString = query_String,
    QueryExecutionContext={
        'Catalog': 'AwsDataCatalog'
    },
    ResultConfiguration={
        'OutputLocation': os.getenv('s3_bucket') ,
    },
    WorkGroup = 'gps'
    )
    return response

def get_execution(client,response):
    get_exec = client.get_query_execution(
        QueryExecutionId=response['QueryExecutionId']
    )
    
    status = get_exec['QueryExecution']['Status']['State']
    
    # if i ask for the results inmediately i'll get an error because the query
    # takes some time to run
    while  status != 'SUCCEEDED':
        get_exec = client.get_query_execution(
            QueryExecutionId=response['QueryExecutionId']
        )
    
        status = get_exec['QueryExecution']['Status']['State']

    response_results = client.get_query_results(
        QueryExecutionId=response['QueryExecutionId']

    )
    
    return response_results

    
#get imps from athena response and creates a pandas dataframe with it
def get_imps(response, response_results):
    athena_imps = response_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    return athena_imps

# calculates total of impressions for yesterdays date
def get_total_imps(imps_table):
    converted = imps_table.iloc[4:28,0]
    imps = 0
    for x in converted:
        imps = imps + int(x)
    return imps

# getting the difference between athena and snowflake for every partner
def compare_imps(athena_imps, snowflake_imps):
    athena_imps = int(athena_imps)
    if athena_imps > 0: 
        diff = ( (athena_imps-snowflake_imps)/athena_imps) * 100
    else:
        diff = 0
    return diff

# Running queries for every partner implementing previous functions
def analize_partners():
    # setting manually the actual measuremente_source_id for snowflake
    partners = {'snapchat': 10,'spotify': 19, 'pinterest': 11,'linkedin': 16,'yahoo': 5}
    partner_imps = pd.DataFrame(columns = ['date','partner', 'athena_imps', 'snowflake_imps', 'difference'])

    #emails class
    new_alert = email_alert()
    #dates for snowflake query
    today = date.today()
    yesterday = today - timedelta(days = 1)
    yesterday = str(yesterday)
    
    #setting athena client
    dates = get_date()
    athena_client = set_client()
    
    #starting snowflake connection
    snowflake_conn = SnowflakeConn()
    cursor = snowflake_conn.start_connection()
    
    ids = 0
    for x,y in partners.items():
        
        #athena
        query_String = f"SELECT count(distinct concat(impressionId, '---', cast(timestamp as varchar))) FROM partner_raw.{x} where type = 'impression' and ((utcdate = '{dates[0]}' and utchour in ('04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23')) or (utcdate = '{dates[1]}' and utchour in ('00', '01', '02', '03')))"
        response = start_query(athena_client, query_String)
        response_results = get_execution(athena_client, response)
        total_imps = get_imps(response, response_results)
        #total_imps = get_total_imps(table_imps)
        print(x,total_imps)
        
        #snowflake
        snowflake_query = f"select HIT_DATE, SUM(IMPS) as IMPS from {os.getenv('snowflake_table')} where HIT_DATE = '{yesterday}' AND measurement_source_id = {y} group by HIT_DATE order by HIT_DATE ASC"
        results = snowflake_conn._execute_query(snowflake_query)
        if results == []:
            results = [(0,0)]
        diffe = compare_imps(total_imps, results[0][1])
        
        partner_imps.loc[ids] = [yesterday,x,total_imps,results[0][1],diffe]
        ids += 1
        
    cursor = snowflake_conn.close_connection()
    new_alert.send_email(f"""Date: {yesterday}\nPartners analysis successfully executed\n\nPlease check: {os.getenv('googlesheet_url')} for more info""")
    # sending the data to check for discreps
    discreps_alert(partner_imps, new_alert)
    return partner_imps

# calculations for dod, wow and mom comparisons
def calculations(selected_row):
    new_alert = email_alert()
    row=selected_row
    count=0
    google_conn = GoogleSheets()
    wk = google_conn.set_worksheet()

    d = wk.get_value('A'+str(row))
    partner,raw,snowflake = 'B','C','D'
    #week-35 
    week_athena,week_snowflake = 'F','G'
    #day-5
    day_athena,day_snowflake = 'H','I'
    #month-155
    month_athena,month_snowflake = 'J','K'
    dod_partners,wow_partners,mom_partners = [],[],[]
    day_string,month_string,week_string  = """""","""""",""""""

    while count < 5:
        if row-35 < 1:
            pass
        else:
            raw_value = wk.get_value(raw+str(row))
            snow_value = wk.get_value(snowflake+str(row))

            week_ago = wk.get_value(raw+str(row-35))
            week_snow = wk.get_value(snowflake+str(row-35))

            raw_value= raw_value.replace(',','')
            week_ago = week_ago.replace(',','')
            snow_value = snow_value.replace(',','')
            week_snow = week_snow.replace(',','')
            
            if int(raw_value) != 0:
                val = ( (int(raw_value) - int(week_ago))/int(raw_value) )
            else:
                val = 0

            val_snowflake = ((int(snow_value) - int(week_snow))/int(snow_value)) 

            wk.update_value(week_athena+str(row),val)
            wk.update_value(week_snowflake+str(row),val_snowflake)

            #print(val,val_snowflake)

            if val < -.10 or val_snowflake < -.10:
                print(val,val_snowflake)
                partner_drop = wk.get_value(partner+str(row))
                wow_partners.append(partner_drop)

                #new_alert.send_email("Drop < -5 for week over week comparisons")

        if row-5 < 1:
            pass
        else:
            raw_value = wk.get_value(raw+str(row))
            snow_value = wk.get_value(snowflake+str(row))
            raw_value= raw_value.replace(',','')
            snow_value = snow_value.replace(',','')

            day_ago = wk.get_value(raw+str(row-5))
            day_snow = wk.get_value(snowflake+str(row-5))

            day_ago = day_ago.replace(',','')
            day_snow = day_snow.replace(',','')
            
            if int(raw_value) != 0:
                day_val = ((int(raw_value) - int(day_ago))/int(raw_value)) 
            else:
                day_val = 0
            
            day_sn = ((int(snow_value) - int(day_snow))/int(snow_value))

            wk.update_value(day_athena+str(row),day_val)
            wk.update_value(day_snowflake+str(row),day_sn)

            #change to -<10
            if day_val < -.10 or day_sn < -.10:
                partner_drop = wk.get_value(partner+str(row))
                dod_partners.append(partner_drop)
    
        if row-155 < 1:
            pass
        else:
            raw_value = wk.get_value(raw+str(row))
            snow_value = wk.get_value(snowflake+str(row))
            raw_value= raw_value.replace(',','')
            snow_value = snow_value.replace(',','')

            month_ago = wk.get_value(raw+str(row-155))
            month_snow = wk.get_value(snowflake+str(row-155))

            month_ago = month_ago.replace(',','')
            month_snow = month_snow.replace(',','')
            
            if int(raw_value) != 0:
                month_val = ((int(raw_value) - int(month_ago))/int(raw_value))
            else:
                month_val = 0

            month_sn = ((int(snow_value) - int(month_snow))/int(snow_value))

            wk.update_value(month_athena+str(row),month_val)
            wk.update_value(month_snowflake+str(row),month_sn)

            if month_val < -.10 or month_sn < -.10:
                partner_drop = wk.get_value(partner+str(row))
                mom_partners.append(partner_drop)
                #new_alert.send_email("Discrepancy < -5 for month over month comparisons")

        row += 1
        count += 1
    if wow_partners:
        week_string = f"""Date: {d}\nDrop higher than 10% detected for: {wow_partners}\nFor: week over week comparisons"""

    if dod_partners:
        day_string = f"""Date: {d}\nDrop higher 10% detected for: {dod_partners}\nFor: Day over day comparisons"""

    if mom_partners:
        month_string = f"""Date: {d}\nDrop higher than 10% detected for: {mom_partners}\nFor: Month over month comparisons"""

    details = f"""Please check: {os.getenv('googlesheet_url')} for more details"""

    if week_string != '' or day_string != '' or month_string != '':
        body = '\n \n'.join([week_string, day_string])
        body = '\n \n'.join([body, month_string])
        body = '\n \n \n'.join([body, details])

        new_alert.send_email(body)

    return

# function to insert impressions and calculations to the googlesheet
def insert_df(partners_df):
    google_conn = GoogleSheets()
    wk = google_conn.set_worksheet()
    
    col = 2
    row = 480
    column = 'B'
    value = wk.cell((row,col)).value
    
    while value != '':
        row +=1
        value = wk.cell((row,col)).value
    
    tmp_df = partners_df.drop(columns = 'date')
    wk.set_dataframe(tmp_df, column+str(row), copy_head = False)
    
    calculations(row)

    return column+str(row)

if __name__ == "__main__":
    new_alert = email_alert()
    try:
        partners_df = analize_partners()
        row = insert_df(partners_df)

    except IndexError:
        new_alert.send_email('An error occurred while executing the script, a partner is not returning impressions from snowflake')
        
