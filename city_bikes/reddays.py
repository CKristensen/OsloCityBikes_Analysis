import holidays
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
import psycopg2
import os


# Database variabler
PASS = os.environ['PASS_P']
USER_NAME = os.environ['USER_NAME_P']
HOST = os.environ['HOST_P']
DATABASE = os.environ['DB_TEAM']


# Get start and end for daterange:
def get_date(yr, mnt, dy, yr2, mnt2, dy2):
    "Function gives datetime start and enddate"
    start_date= dt.datetime(year=yr, month=mnt, day=dy)
    end_date= dt.datetime(year=yr2, month=mnt2, day=dy2)
    return start_date, end_date

# Doing manually:
# start_date= dt.datetime(year=2020, month=9, day=1)
# end_date= dt.datetime(year=2020, month=9, day=30)

def get_reddays(yr,start_date, end_date):
    '''Function that gives out holidays for a given period,
    stored as df and sends to sql-db'''
    red_days= [str(date[0]) for date in holidays.NOR(years=yr).items()]

    # Create df with date. Change from timestamp to date
    df= pd.DataFrame()
    df['Date']= pd.date_range(start_date, end_date)
    df['Dates']=df['Date'].dt.date
    del df['Date']

    # Get holidays-column
    df['is_holiday']=[1 if str(i).split()[0] in red_days else 0 for i in df['Dates']]

    # Send to sql-db via create engine
    engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
    df.to_sql('tablename', engine, index=False, schema='bysykkel', if_exists='replace')
    
    return df, red_days



# Henter dato og helligdager for år 2016-2022:
start,end = get_date(2016,4,1,2016,12,31)
df16, reds=get_reddays(2016, start, end)

start,end = get_date(2017,1,1,2017,12,31)
df17, reds=get_reddays(2017, start, end)

start,end = get_date(2018,1,1,2018,12,31)
df18, reds=get_reddays(2018, start, end)

start,end = get_date(2019,1,1,2019,12,31)
df19, reds=get_reddays(2019, start, end)

start,end = get_date(2020,1,1,2020,12,31)
df20, reds=get_reddays(2020, start, end)

start,end = get_date(2021,1,1,2021,12,31)
df21, reds=get_reddays(2021, start, end)

start,end = get_date(2022,1,1,2022,12,31)
df22, reds=get_reddays(2022, start, end)


# Concater alle df for ulike år i ett.
df_all_yr= pd.concat([df16, df17, df18, df19, df20, df21, df22],axis=0)
    

# Sender til sql-db via postgres & create engine
engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
df_all_yr.to_sql('reddays 16-22', engine, index=False, schema='bysykkel', if_exists='replace')



