import os
from datetime import date,timedelta, datetime
import pygsheets
import pandas as pd

class GoogleSheet():
    
    def __init__(self):
        # class constructor
        #initiates connection with google
        file = './client_secret.json'
        url = os.getenv('googlesheet_url')

        self.gc = pygsheets.authorize(service_account_file=file)
        self.sh = self.gc.open_by_url(url)

    def get_worksheet(self, worksheet_name):
        # returning existing worksheet or creating one if doesnt exist
        try:
            worksheet = self.sh.worksheet_by_title(worksheet_name)
        except pygsheets.WorksheetNotFound:
            print('Creating worksheet')
            worksheet = self.sh.add_worksheet(worksheet_name)
            print('worksheet created')
        
        return worksheet
    
    def get_worksheet_data(self, worksheet):
        # Accessing the google worksheet and extracting it as pandas DataFrame
        df = worksheet.get_as_df()
        return df
    
    def join_data(self, data, df, today):
        # calculating dod, mom and wow
        # joining new calculations dataframe with old dataframe from the worksheet
        if df.values.tolist() == []:

            df_new = pd.concat([df, data], ignore_index=True,sort = False).fillna(' ')
            return df_new, data.values.tolist()
        
        else:

            values = data.values.tolist()

            date_ev = today - timedelta(days = 1)
            df_old = df.loc[df['Date'] == date_ev.strftime("%Y-%m-%d")]
            day = df_old.values.tolist()

            week_ev = today - timedelta(days = 7)
            df_old = df.loc[df['Date'] == week_ev.strftime("%Y-%m-%d")]
            week = df_old.values.tolist()

            month_ev = today - timedelta(days = 31)
            df_old = df.loc[df['Date'] == month_ev.strftime("%Y-%m-%d")]
            month = df_old.values.tolist()

            for x in range(len(day)):
                for y in range(len(day[x])):
                    if day:
                        if ',' in str(day[x][y]):
                            day[x][y] = day[x][y].replace(',','')

                        if week:
                            if ',' in str(week[x][y]):
                                week[x][y] = week[x][y].replace(',','')
    
                            if month:   
                                if ',' in str(month[x][y]):
                                    month[x][y] = month[x][y].replace(',','')

            for i in range(0,len(values)):
                if int(values[i][2]) == 0:
                    day_athena, week_athena, month_athena = 0,0,0
    
                else:
                    if day:
                        day_athena = round(((int(values[i][2]) - int(day[i][2]))/int(values[i][2])) * 100,3)

                        if week:
                            week_athena = round(((int(values[i][2]) - int(week[i][2]))/int(values[i][2])) * 100,3)
                
                            if month:
                                month_athena = round(((int(values[i][2]) - int(month[i][2]))/int(values[i][2])) * 100,3)
                            else:
                                month_athena = 0
                        else:
                            week_athena = 0
                            month_athena = 0
                    else:
                        day_athena = 0
    
                if int(values[i][3]) == 0:
                    day_snow,week_snow,month_snow = 0,0,0
    
                else:
                    if day:
                        day_snow = round(((int(values[i][3]) - int(day[i][3]))/int(values[i][3])) * 100,3)
                        if week:
                            week_snow = round(((int(values[i][3]) - int(week[i][3]))/int(values[i][3])) * 100,3)
                            if month:
                                month_snow = round(((int(values[i][3]) - int(month[i][3]))/int(values[i][3])) * 100,3)
                            else:
                                month_snow = 0
                        else:
                            week_snow = 0
                            month_snow = 0
                    else:
                        day_snow = 0
    
                values[i].extend([week_athena, week_snow, day_athena, day_snow, month_athena, month_snow])
    
            df_calc = pd.DataFrame(values, columns = ['Date', 'Partner','Partner_raw','Snowflake','%_diff_between_Snowflake_and_partner_raw',
                                            '%_diff_week_athena','%_diff_week_snowflake','%_diff_day_athena','%_diff_day_snowflake',
                                            '%_diff_month_athena','%_diff_month_snowflake'])
            df_new = pd.concat([df, df_calc], ignore_index=True,sort = False).fillna(' ')
    
            return df_new, values
    
    def update_sheet(self, df, worksheet):
        # updating the worksheet with new dataframe
        worksheet.set_dataframe(df, (1,1), copy_index = False,fit=True)
        return True
