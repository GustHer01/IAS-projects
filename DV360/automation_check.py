import csv
import email
import sys
#import logging
from cleaning import cleaning_csv
from cleaning import check_delim
from upload_to_s3 import upload_file
from pull_from_s3 import pull_file
from alerts.emails import email_alert
from dotenv import find_dotenv, load_dotenv
#from logs.logger import logger as lg

def read_file():
    file = open('./csv/ias_rates.csv')
    content = file.readlines()
    
    
    first_line = content[0]
    last_line = content[-1]

    return first_line, last_line, content

#getting existing currencies
def get_currencies():
    with open('./csv/currency_codes.csv', 'r') as file:
        reader = csv.reader(file)
        codes_data = []

        for row in reader:
            codes_data.append(row[2])
    codes_data.pop(0)
    return codes_data

#getting currencies on ias_rates file
def get_ias_currencies():
    with open('./csv/ias_rates.csv', 'r') as file:
        reader = csv.reader(file)
        codes_data = []

        for row in reader:
            codes_data.append(row[2])
    codes_data.pop(0)
    return codes_data

def check_currencies(actual_currencies, ias_currencies):
    new_alert = email_alert()
    errors = []
    count_line = 1
    for currency in ias_currencies:
        count_line += 1
        if currency not in actual_currencies:
            errors.append(count_line)
    
    if errors:
        error_string = f"""Bad currency detected on lines {errors}\n\nPlease check the ias_rates.csv file"""
        new_alert.send_email(error_string, 'RATE CHECK - BAD CURRENCY')


def check_line( first, last, content, upload):
    #logger = lg.logger(logging.DEBUG)
    errors, cleaning_string = [], " "
    new_alert = email_alert()

    schema = {
        0: int,
        1: str,
        2: str
    }


    columns = first.split(',')
    #columns.pop(-1)
    columns[:] = [x for x in columns if x]
    columns_len = len(columns)
    for x in range(3,12):
        schema[x] = [int,float]

    first_line = [i for i in first.split(',')]


    line_count = 1
    content.pop(0)
    spaces = False
    for line in content:
        line_count +=1
        last_line = [int(i) if i.isdigit() else float(i) if i.isdigit() == False and '.' in i else i for i in line.split(',')]
        
        for x in range(0, 3):
            if not last_line[x]:
                errors.append((columns[x],line_count))
            elif type(last_line[x]) != schema[x]:
                errors.append((columns[x], line_count))
        
        for x in range(3,12):
            if not last_line[x]:
                if last_line[x] == 0:
                    pass
                else:
                    errors.append((columns[x], line_count))
            elif type(last_line[x]) not in schema[x]:
                last_line[x] = last_line[x].strip()
                if last_line[x].isdigit() == False:
                    errors.append((columns[x], line_count))
        if '' in last_line:
            spaces = True

    if ''  in first_line or spaces == True or upload == True:
        #logger.info('Extra commas found, cleaning file...')
        cleaning_csv('./csv/ias_rates.csv')
            
        #logger.info('Uploading to s3')
        cleaning_string = 'Extra commas or different delimiter found\n\nCSV file cleaned and uploaded to S3\nPlease double check that the format of the file is correct'
        new_alert.send_email(cleaning_string, 'RATE CHECK - Cleaned File')
        upload_file('./csv/ias_rates.csv')
    
    
    if errors:
        body = """"""
        for error in errors:
            errors_string = f"""Type error(s) detected for column: {error[0]} on line: {error[1]}\n"""
            body = '\n\n'.join([body,errors_string])
        check_file_string = 'Please check ./csv/ias_rates.csv file'
        body = '\n\n'.join([body,check_file_string])
        
        new_alert.send_email(body, 'RATE CHECK')

    else:
        check_passed_string = 'Rate check successfully passed\nNo typo errors detected'
        new_alert.send_email(check_passed_string, 'RATE CHECK')
    
    
    
if __name__ == "__main__":
    load_dotenv(find_dotenv('config.env'))
    filename = './csv/ias_rates.csv'
    pull_file(filename)

    upload = check_delim(filename)

    lines = read_file()
    check_line(lines[0], lines[1], lines[2],upload)

    actual_currrencies = get_currencies()
    ias_rates_currencies = get_ias_currencies()
    check_currencies(actual_currrencies,ias_rates_currencies)

