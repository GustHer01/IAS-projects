import csv

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



