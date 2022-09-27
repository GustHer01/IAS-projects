import csv
import email
import sys
import logging
from cleaning import cleaning_csv
from upload_to_s3 import upload_file
from pull_from_s3 import pull_file
from alerts.emails import email_alert
from dotenv import find_dotenv, load_dotenv
from logs.logger import logger as lg

def read_file():
    file = open('ias_rates.csv')
    content = file.readlines()

    first_line = content[0]
    last_line = content[-1]
   
    return first_line, last_line

def check_line(first,last):
    logger = lg.logger(logging.DEBUG)
    errors, cleaning_string = [], " "
    new_alert = email_alert()

    schema = {
        0: int,
        1: str,
        2: str
    }
    for x in range(3,12):
        schema[x] = float

    columns = first.split(',')
    columns.pop(-1)
    columns[:] = [x for x in columns if x]
    columns_len = len(columns)

    first_line = [i for i in first.split(',')]
    last_line = [int(i) if i.isdigit() else float(i) if i.isdigit() == False and '.' in i else i for i in last.split(',')]

    for x in range(0, columns_len):
        if not last_line[x]:
            errors.append(columns[x])
        elif type(last_line[x]) != schema[x]:
            errors.append(columns[x])

    if ''  in first_line and ''  in last_line:
        logger.info('Extra commas found, cleaning file...')
        cleaning_csv('ias_rates.csv')
        logger.info('Uploading to s3')
        cleaning_string = 'CSV file cleaned and uploaded successfully'
        #upload_file('ias_rates.csv')
    
    
    if errors:
        errors_string = f"""Type error(s) detected for columns {errors}\n\nPlease check ias_rates.csv file"""
        body = '\n \n'.join([errors_string,cleaning_string])

        new_alert.send_email(body, 'RATE CHECK')
    else:
        print(cleaning_string)
        check_passed_string = 'Rate check successfully passed'
        body = '\n \n'.join([check_passed_string, cleaning_string])
        new_alert.send_email(body, 'RATE CHECK')
    
    
if __name__ == "__main__":
    load_dotenv(find_dotenv('config.env'))
    pull_file('ias_rates.csv')
    lines = read_file()
    check_line(lines[0], lines[1])
