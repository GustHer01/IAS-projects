#!/usr/bin/env python
# coding: utf-8
import os
import logging
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from alerts.email_test import email_alert
from dotenv import find_dotenv, load_dotenv
from snowflake_conn.snowflake import SnowflakeConn
from GoogleSheets.google_sheets import GoogleSheet
from datetime import date, timedelta
from tools.calculations import *
import tools.logger as lg
from tools.athena import athena_connection

# Given a df with the impressions from athena and snowflake check for discreps between the sources
def discreps_alert(partners, alert_obj):
    discreps = []
    for x in range(len(partners)):
        if int(partners['%_diff_between_Snowflake_and_partner_raw'].loc[x]) > 10 or int(partners['%_diff_between_Snowflake_and_partner_raw'].loc[x]) < -10:
            discreps.append(partners.Partner.loc[x])
    if discreps:
        alert_obj.send_email(f"""Date: {partners.Date.loc[1]}\nDiscrep > than 10% detected for: {discreps}\nPlease check: Please check: {os.getenv('googlesheet_url')} for details""", 'PMI-ALERTS')
    return 

# Formats the dates to athena format
def get_date(yesterday ,today):
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
def analize_partners(dates,yesterday,today):
    logger = lg.logger(logging.DEBUG)
    logger.info('Process analysis Started...')
    # setting manually the actual measuremente_source_id for snowflake
    #partners = {'snapchat': 10,'spotify': 19, 'pinterest': 11,'linkedin': 16,'yahoo': 5}
    partners = {'snapchat':[10,1,'snapchat'], 'spotify':[19,2,'general_events_v2'], 'pinterest': [11,2,'general_events_v1'], 'linkedin': [16,2,'general_events_v1'], 'yahoo':[5,1,'yahoo']}

    partner_imps = pd.DataFrame(columns = ['Date','Partner', 'Partner_raw', 'Snowflake', '%_diff_between_Snowflake_and_partner_raw'])

    # setting the client
    #athena_client = set_client()
    athena_obj = athena_connection()
    #starting snowflake connection
    snowflake_conn = SnowflakeConn()
    cursor = snowflake_conn.start_connection()
    
    ids = 0
    for x,y in partners.items():
        
        # starts athena query with the following query string

        if y[1] == 1:
            query_String = f"""SELECT count(distinct concat(impressionId, '---', cast(timestamp as varchar))) 
                               FROM {os.getenv('athena_db')}.{y[2]} 
                               where type = 'impression' and ((utcdate = '{dates[0]}' and utchour in ('04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23')) 
                               or (utcdate = '{dates[1]}' and utchour in ('00', '01', '02', '03')));"""
        elif y[1] == 2:
            query_String = f"""SELECT count(distinct concat("original.impressionId", '---', cast("original.timestamp" as varchar))) 
                            FROM "{os.getenv('athena_db')}".{y[2]}
                            where "original.type" = 'impression' and ((utcdate = '{dates[0]}'and utchour in ('04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23')) 
                            or (utcdate = '{dates[1]}' and utchour in ('00', '01', '02', '03')))
                            and sourceid = {y[0]};"""
        # starting query
        response = athena_obj.start_query(query_String)
        # getting the execution result
        response_results = athena_obj.get_execution(response)
        total_imps = athena_obj.get_imps(response_results)
        
        logger.info(f'Partner processed: {x} ')
        logger.info(f'Impressions from athena: {total_imps}')
        
        #snowflake
        snowflake_query = f"select HIT_DATE, SUM(IMPS) as IMPS from {os.getenv('snowflake_table')} where HIT_DATE = '{yesterday}' AND measurement_source_id = {y[0]} group by HIT_DATE order by HIT_DATE ASC"
        results = snowflake_conn._execute_query(snowflake_query)
        if results == []:
            results = [(0,0)]
        diffe = compare_imps(total_imps, results[0][1])
        
        # stores the results in a pandas dataframe
        partner_imps.loc[ids] = [yesterday,x,total_imps,results[0][1],diffe]
        ids += 1
    
    # closing connection to snowflake
    cursor = snowflake_conn.close_connection()
    return partner_imps

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    logger = lg.logger(logging.DEBUG)

    # instance of email and googlesheets objects
    new_alert = email_alert()
    gc = GoogleSheet()
    
    # date to process
    day = date.today() - timedelta(days = 1)

    day_after = day + timedelta(days = 1)
    athena_formatted_dates = get_date(day, day_after)
    email_day = day.strftime('%Y-%m-%d')
    logger.info(f'Processed date: {email_day}')

    # Executing analysis
    partners_df = analize_partners(athena_formatted_dates, day, day_after)

    #getting previus info in the worksheet
    worksheet = gc.get_worksheet('Partner-Results')
    df_old = gc.get_worksheet_data(worksheet)
    # joining old and new info
    df_new, data = gc.join_data(partners_df, df_old, day)
    # updating the worksheet
    gc.update_sheet(df_new, worksheet)
    logger.info('Process finished... Checking for drops and sending emails')
    new_alert.send_email(f"""Date: {email_day}\nPartners analysis successfully executed\n\nPlease check: {os.getenv('googlesheet_url')} for more info""", 'PMI-ALERTS')
    # check for discreps
    discreps_alert(partners_df,new_alert)
    # checking for drops
    filter_values(data, email_day, new_alert)        