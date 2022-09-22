import os

# checks for drops
def check_drops(data,values):
    partners = set()
    for x in range(len(data)):
        for y in data[x]:
            if y < -10:
                partners.add(values[x][1])
    
    return partners

# dividing data in days, weeks and months to check for drops
def filter_values(values,email_day,new_alert):
    if values[0][5] and values[0][6]:
        week_drops = [x[5:7] for x in values]
        week_d = check_drops(week_drops, values)
    
    if values[0][7] and values[0][8]:
        day_drops = [x[7:9] for x in values]
        day_d = check_drops(day_drops, values)

    if values[0][9] and values[0][10]:
        month_drops = [x[9:11] for x in values]
        month_d = check_drops(month_drops, values)
    
    if week_d:
        week_string = f"""Date: {email_day}\nDrop higher than 10% detected for: {week_d}\nFor: week over week comparisons"""

    if day_d:
        day_string = f"""Date: {email_day}\nDrop higher 10% detected for: {day_d}\nFor: Day over day comparisons"""

    if month_d:
        month_string = f"""Date: {email_day}\nDrop higher than 10% detected for: {month_d}\nFor: Month over month comparisons"""

    details = f"""Please check: {os.getenv('googlesheet_url')} for more details"""

    if week_string != '' or day_string != '' or month_string != '':
        body = '\n \n'.join([week_string, day_string])
        body = '\n \n'.join([body, month_string])
        body = '\n \n \n'.join([body, details])

        new_alert.send_email(body,'PMI-ALERTS')