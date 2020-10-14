from zipfile import ZipFile
import db_operations as db
import pandas as pd
import requests
import re
import os
from datetime import date, timedelta
import numpy as np

# Insert your own client ID here
CLIENT_ID = os.environ['FROST_API_CLIENT_ID']

def get_weather_station_latlon():
    """Goes into FrostAPI and gets all the stations unique SN
    numbers in Oslo, their names and their coordinates.
    returns: pandas Dataframe with columns 'id', 'name', 'lat', 'lon':
    """
    # Define endpoint and parameters
    endpoint = 'https://frost.met.no/sources/v0.jsonld'
    parameters = {
        'county': 'Oslo'
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID, ''))
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

    df = pd.DataFrame(columns=["id", "name", "geometry"])

    for row in data:
        df.loc[len(df)] = [row['id'], row['name'], row["geometry"]]

    # extract the latitude and longitude from the geometry column
    lat = lambda x: re.findall(r'(\d+\.\d+)', str(x))[1]
    lon = lambda x: re.findall(r'(\d+\.\d+)', str(x))[0]
    df['lat'] = df['geometry'].apply(lat)
    df['lon'] = df['geometry'].apply(lon)

    return df[['id', 'name', 'lat', 'lon']]

def get_weather_on_station(sourceId, start_date):
    """Get the weather on a particular station
    Args:
        sourceId (str): sourceId for a weather station in FrostAPI
    Returns:
        Pandas Dataframe: columns = sourceID, referenceTime, elementId, value, unit
    """
    today = str(date.today())
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},',
        'elements': 'wind_speed',
        'referencetime': f'{start_date}/{today}',
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID, ''))
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
        # [i]['observations'])
        # row['referenceTime'] = data[i]['referenceTime']
        # row['sourceId'] = data[i]['sourceId']
        df = df.append(row)
    # df = df.reset_index()

    # datef = lambda x: x[:10].replace('-','')
    # timef = lambda x: x[11:13]
    # sfunc = lambda x: x[:-2]

    # df['date'] = df['referenceTime'].apply(datef)
    # df['time'] = df['referenceTime'].apply(timef)
    # df['sourceId'] = df['sourceId'].apply(sfunc)
    # df = df[df['qualityCode']==0.0]
    # df = df.drop_duplicates(subset=['sourceId', 'time', 'date'])

    return df  # [['sourceId', 'time', 'date', 'value', 'unit']]

def insert_weather_stations_latlon():
    temp_prec_ids = ['SN18210', 'SN18815', 'SN18315', 'SN18700']
    temp_prec = get_weather_station_lat_lon_from_list(temp_prec_ids)

    wind_ids = ['SN18210', 'SN18700', 'SN76914']
    wind = get_weather_station_lat_lon_from_list(wind_ids)
    conn, cur = db.connect()

    arg_list = []
    for _, arg in temp_prec.iterrows():
        arg_list.append((str(arg['id']), str(
            arg['name']), str(arg['lat']), str(arg['lon'])))

    query = '''insert into weather.prec_temp_stations(id_, name_, lat_, lon_)
                        values (%s, %s, %s, %s)'''
    cur.executemany(query, arg_list)

    arg_list = []
    for _, arg in wind.iterrows():
        arg_list.append((str(arg['id']), str(
            arg['name']), str(arg['lat']), str(arg['lon'])))

    query = '''insert into weather.wind_stations(id_, name_, lat_, lon_)
                        values (%s, %s, %s, %s) ON CONFLICT DO NOTHING'''

    cur.executemany(query, arg_list)

    conn.commit()
    db.close(conn, cur)
    return 1

def init_database():
    conn, cur = db.connect()
    with open('init_db.sql', 'r') as sql_file:
        cur.execute(sql_file.read())
    db.close(conn, cur)

def get_wind_speed_by_hour(dates, sourceId = 'SN18700'):
    
    # today = str(date.today())
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'wind_speed',
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
    df['wind_speed_ms'] = df['observations'].apply(get_value)

    get_hour = lambda x: re.search(r"T(\d\d):", x).group(1)
    get_date = lambda x: re.search(r"^(\d{4}-\d{2}-\d{2})T", x).group(1)
    df['hour'] = df['referenceTime'].apply(get_hour)
    df['date'] = df['referenceTime'].apply(get_date)

    df['id'] = df['sourceId'].apply(lambda x: x[:-2])
    df = df.drop(columns=['observations', 'referenceTime', 'sourceId'])

    df = df.groupby(by=['date', 'hour', 'id']).mean()
    df = df.reset_index()
    return df

def get_temperatur_by_hour(dates, sourceId='SN18700'):

    # today = str(date.today())
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'air_temperature',
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
    df['air_temperatur_celsius'] = df['observations'].apply(get_value)

    get_hour = lambda x: re.search(r"T(\d\d):", x).group(1)
    get_date = lambda x: re.search(r"^(\d{4}-\d{2}-\d{2})T", x).group(1)
    df['hour'] = df['referenceTime'].apply(get_hour)
    df['date'] = df['referenceTime'].apply(get_date)

    df['id'] = df['sourceId'].apply(lambda x: x[:-2])
    df = df.drop(columns=['observations', 'referenceTime', 'sourceId'])

    df = df.groupby(by=['date', 'hour', 'id']).mean()
    df = df.reset_index()
    return df

def get_precipitation_by_day(dates, sourceId='SN18700'):
    
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
    return df

def get_weather_station_lat_lon_from_list(ids):
    df = get_weather_station_latlon()
    df = df[df['id'].isin(ids)]
    return df

def iter_dates():
    for year in range(2016, 2021):
        for month, next_month in zip(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], ['02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '01']):
            next_year = year
            if(next_month == '01'):
                next_year+= 1
            if((year == 2016) & (int(month) < 3)):
                continue
            if((year == 2020) & (int(month)==9)):
                break
            yield f'{year}-{month}-02/{next_year}-{next_month}-01'

def insert_wind_speed_by_hour(dates = '2020-10-09/2020-10-10'):
    df_ = get_wind_speed_by_hour(dates)
    conn, cur = db.connect()
    arg_list = []
    for _, arg in df_.iterrows():
        arg_list.append((str(arg['id']), str(
            arg['hour']), str(arg['date']), str(arg['wind_speed_ms'])))

    query = '''insert into weather.wind(id_, hour_, date_, wind_speed_ms)
                        values (%s, %s, %s, %s) ON CONFLICT DO NOTHING'''
    cur.executemany(query, arg_list)
    db.close(conn, cur)

def insert_temperatur_by_hour(dates = '2020-10-09/2020-10-10'):
    df_ = get_temperatur_by_hour(dates)
    conn, cur = db.connect()
    arg_list = []
    for _, arg in df_.iterrows():
        arg_list.append((str(arg['id']), str(
            arg['hour']), str(arg['date']), str(arg['air_temperatur_celsius'])))

    query = '''insert into weather.temperature(id_, hour_, date_, air_temperatur_celsius)
                        values (%s, %s, %s, %s) ON CONFLICT DO NOTHING'''
    cur.executemany(query, arg_list)
    db.close(conn, cur)

def insert_precipitation_by_day(dates = '2020-09-09/2020-10-10'):
    df_ = get_precipitation_by_day(dates)
    conn, cur = db.connect()
    arg_list = []
    for _, arg in df_.iterrows():
        arg_list.append((str(arg['id']), str(arg['date']), str(arg['precipitation_mm'])))

    query = '''insert into weather.precipitation(id_,  date_, precipitation_mm)
                        values (%s, %s, %s) ON CONFLICT DO NOTHING'''
    cur.executemany(query, arg_list)
    db.close(conn, cur)

# dates = '2020-09-01/2020-10-01'
# init_database()
# insert_wind_speed_by_hour(dates)
# insert_temperatur_by_hour(dates)
# insert_precipitation_by_day(dates)

for dates in iter_dates():  
    insert_wind_speed_by_hour(dates)
    insert_temperatur_by_hour(dates)
    insert_precipitation_by_day(dates)