import csv
import pandas as pd
from detect_delimiter import detect

def check_delim(filename):
    file = open(filename)
    content = file.readlines()

    first_line = content[0]
    delim = detect(first_line)
    if delim == ',':
        return False
    else:
        wrong_delim = pd.read_csv(filename, sep = delim)
        wrong_delim.to_csv(filename, index=False)

        return True

def cleaning_csv(filename):
    rows = []
    with open(filename, encoding='utf-8') as f:
        lines = csv.reader(f)
        for row in lines:
            row = [x for x in row if x]
            rows.append(row)

    with open(filename, 'w') as f:
        writer = csv.writer(f)

        for x in rows:
            writer.writerow(x)
    
    return



