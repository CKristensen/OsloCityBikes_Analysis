import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime, date, timedelta
import requests
import re

PASS = os.environ['DB_PASS']
USER = os.environ['DB_USER']
DBNAME = os.environ['DB_NAME']
HOST = os.environ['DB_URL']
CLIENT_ID = os.environ['FROST_API_CLIENT_ID']

def get_yesterday_rides(days_ago=2):
    OBS_ROOT = "https://data.urbansharing.com/oslobysykkel.no/trips/v1/2020/10.json"
    resource = requests.get(OBS_ROOT)
    yestoday = str(datetime.now() - timedelta(days=days_ago))[:10]
    data = resource.json()
    df = pd.DataFrame(data)
    df['date'] = df['started_at'].apply(lambda x: x[:10])
    df = df[df['date'] == yestoday].reset_index()
    df  = df[['started_at', 'ended_at', 'duration', 'start_station_id', 'end_station_id']]
    df.to_csv('ridesy.csv', index=False)
    assert len(df.index) > 100
    return df

def get_wind_yesterday(sourceId = 'SN18700', days_ago=2):
    
    yesterday = str(datetime.now() - timedelta(days=(days_ago)))[:10]
    today = str(datetime.now() - timedelta(days=days_ago-1))[:10]
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'wind_speed',
        'referencetime': f'{yesterday}/{today}',
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID,''))
    # Extract JSON data
    json = r.json()
    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        print('Data retrieved from frost.met.no!')
    else:
        print('Error! Returned status code %s' % r.status_code)
        print('Message: %s' % json['error']['message'])
        print('Reason: %s' % json['error']['reason'])
        return None

    df = pd.DataFrame()
    for i in range(len(data)):
        row = pd.DataFrame(data[i])
        df = df.append(row)
    get_value = lambda x: x['value']
    df['wind_speed_ms'] = df['observations'].apply(get_value)

    get_hour = lambda x: int(re.search(r"T(\d\d):", x).group(1))
    get_date = lambda x: re.search(r"^(\d{4}-\d{2}-\d{2})T", x).group(1)
    df['hour'] = df['referenceTime'].apply(get_hour)
    df['date'] = df['referenceTime'].apply(get_date)

    df['id'] = df['sourceId'].apply(lambda x: x[:-2])
    df = df.drop(columns=['observations', 'referenceTime', 'sourceId'])

    df = df.groupby(by=['date', 'hour', 'id']).mean()
    df = df.reset_index()
    df.to_csv('windy.csv', index=False)
    assert len(df.index) == 24
    return df

def get_temperature_yesterday(sourceId = 'SN18700', days_ago=2):
    yesterday = str(datetime.now() - timedelta(days=days_ago))[:10]
    today = str(datetime.now() - timedelta(days=days_ago-1))[:10]
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'air_temperature',
        'referencetime': f'{yesterday}/{today}',
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID,''))
    # Extract JSON data
    json = r.json()
    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        print('Data retrieved from frost.met.no!')
    else:
        print('Error! Returned status code %s' % r.status_code)
        print('Message: %s' % json['error']['message'])
        print('Reason: %s' % json['error']['reason'])
        return None

    df = pd.DataFrame()
    for i in range(len(data)):
        row = pd.DataFrame(data[i])
        df = df.append(row)
    get_value = lambda x: x['value']
    df['air_temperatur_celsius'] = df['observations'].apply(get_value)

    get_hour = lambda x: int(re.search(r"T(\d\d):", x).group(1))
    get_date = lambda x: re.search(r"^(\d{4}-\d{2}-\d{2})T", x).group(1)
    df['hour'] = df['referenceTime'].apply(get_hour)
    df['date'] = df['referenceTime'].apply(get_date)

    df['id'] = df['sourceId'].apply(lambda x: x[:-2])
    df = df.drop(columns=['observations', 'referenceTime', 'sourceId'])

    df = df.groupby(by=['date', 'hour', 'id']).mean()
    df = df.reset_index()

    df.to_csv('temperaturey.csv', index=False)
    assert len(df.index) == 24
    return df

def get_precipitation_yesterday(sourceId = 'SN18700', days_ago=2):
    yesterday = str(datetime.now() - timedelta(days=days_ago))[:10]
    today = str(datetime.now() - timedelta(days=days_ago-1))[:10]
    dates = f'{yesterday}/{today}'
    # today = str(date.today())
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'sum(precipitation_amount P1D)',
        'referencetime': dates,
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID,''))
    # Extract JSON data
    json = r.json()
    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        print('Data retrieved from frost.met.no!')
    else:
        print('Error! Returned status code %s' % r.status_code)
        print('Message: %s' % json['error']['message'])
        print('Reason: %s' % json['error']['reason'])
        return None

    df = pd.DataFrame()
    for i in range(len(data)):
        row = pd.DataFrame(data[i])
        df = df.append(row)
    get_value = lambda x: x['value']
    df['precipitation_mm'] = df['observations'].apply(get_value)

    get_date = lambda x: re.search(r"^(\d{4}-\d{2}-\d{2})T", x).group(1)
    df['date'] = df['referenceTime'].apply(get_date)

    df['id'] = df['sourceId'].apply(lambda x: x[:-2])
    df = df.drop(columns=['observations', 'referenceTime', 'sourceId'])

    df = df.groupby(by=['date', 'id']).mean()
    df = df.reset_index()
    df.to_csv('precipitationy.csv', index=False)
    assert len(df.index) == 1
    return df

def transform_yesterdays_data():
    rides = pd.read_csv('ridesy.csv')
    temperature = pd.read_csv('temperaturey.csv')
    wind = pd.read_csv('windy.csv')
    precipitation = pd.read_csv("precipitationy.csv")

    rides['hour'] = rides['started_at'].apply(lambda x: int(x[11:13]))
    
    rides = rides.merge(temperature[['hour', 'air_temperatur_celsius']], on='hour')
    rides = rides.merge(wind[['hour', 'wind_speed_ms']], on='hour')
    rides['precipitation_mm'] = precipitation['precipitation_mm'][0]
    rides.to_csv('day_rides.csv')

def delete_temp_data():
    try:
        os.remove('ridesy.csv')
    except:
        print("Didn't find rides file")
    try:
        os.remove('windy.csv')
    except:
        print("Didn't find wind file")
    try:
        os.remove('temperaturey.csv')
    except:
        print("Didnt't find temperature file")
    try:
        os.remove('precipitationy.csv')
    except:
        print("Didn't find precipitation file")

def data_into_db():
    day_information = pd.read_csv('day_rides.csv')
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASS}@{HOST}/{DBNAME}')
    day_information.to_sql('day_dump', engine, index=False, schema='airflow', if_exists='replace', method='multi', chunksize=1000)
    return 1

def transform_in_db():
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASS}@{HOST}/{DBNAME}')
    with engine.connect() as connection:
        connection.execute('''
                                delete from star."bikeTrip"
                                where start_date = (select cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) 
                                from airflow.day_dump dd 
                                group by cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int));''')
    
        connection.execute('''
        insert into star."bikeTrip"(start_station_id, end_station_id, start_date, start_time, end_date, end_time, duration, air_temperatur_celsius, wind_speed_ms, precipitation_mm)
                            select start_station_id, 
                            end_station_id, 
                            cast(to_char(cast(started_at  as date), 'YYYYMMDD') as int) as start_date, 
                            cast(to_char(cast(started_at  as time), 'HH24MI') as int) as start_time, 
                            cast(to_char(cast(ended_at  as date), 'YYYYMMDD') as int) as end_date, 
                            cast(to_char(cast(ended_at  as time), 'HH24MI') as int) as end_time, 
                            cast(duration as int),
                            cast(air_temperatur_celsius as float),
                            cast(wind_speed_ms as float),
                            cast(precipitation_mm as float) from airflow.day_dump dd;''')

def ping_postgres():
    test = ['hello', time.time()]
    test_pandas = pd.DataFrame([test], columns=['what', 'who'])

    # Create engine for db-connection:
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASS}@{HOST}/{DBNAME}')
    test_pandas.to_sql('test', engine, index=False, schema='bysykkel', if_exists='append')
    print("Sending data to db!")

if __name__=='__main__':
    pass
    # for dag in range(3, 19):
    #     get_yesterday_rides(days_ago=dag)
    #     get_wind_yesterday(days_ago=dag)
    #     get_temperature_yesterday(days_ago=dag)
    #     get_precipitation_yesterday(days_ago=dag)
    #     transform_yesterdays_data()
    #     data_into_db()
    #     transform_in_db()
    #     delete_temp_data()