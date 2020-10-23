import os
import psycopg2
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from zipfile import ZipFile

PASS = os.environ['PASS_P']
USER_NAME = os.environ['USER_NAME_P']
HOST = os.environ['HOST_P']
DATABASE = os.environ['DB_TEAM']

def get_legacy_month_to_db(date, tablename):
    '''Function to load data from website as json, save it as dataframe,
        and send the dataframe to postgres-db'''
    
    OBS_ROOT= "https://data-legacy.urbansharing.com/oslobysykkel.no/"
    resource = requests.get(OBS_ROOT + date)
    data = pd.DataFrame()

    with open('test_legacy.zip', 'ab') as fp:
        b = resource.content
        fp.write(b)

    with ZipFile('test_legacy.zip') as myzip:
        myzip.extractall()
        json_files = [pos_json for pos_json in os.listdir(os.getcwd()) if pos_json.endswith('.csv')]
        for json in json_files:
            print(json_files)
            test_json = pd.read_csv(str(os.getcwd()).replace('\\', '/')+'/' + str(json))
            data = test_json
            os.remove(json)
            break
    os.remove('test_legacy.zip')

    data['Start time'] = data['Start time'].apply(lambda x: x[:-6])
    data['End time'] = data['End time'].apply(lambda x: x[:-6])
    print(str(date)+' has rows: '+ str(len(data.index)))
    # Create engine for db-connection:
    engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
    data.to_sql(tablename, engine, index=False, schema='bysykkel', if_exists='append', method='multi', chunksize=1000)
    print("Sending data to db!")
    
    return 1

def get_all_stations(tablename='stations'):
    '''Function to load data from website as json, save it as dataframe,
    and send the dataframe to postgres-db'''
    all_stations = pd.DataFrame(columns=['end_station_id', 'end_station_name', 'end_station_description', 'end_station_latitude', 'end_station_longitude'])
    
    for date in iter_dates():
        OBS_ROOT= "https://data.urbansharing.com/oslobysykkel.no/trips/v1/"
        resource = requests.get(OBS_ROOT + date)
        data = resource.json()
        try:
            df = pd.DataFrame(data)
            df = df[['end_station_id', 'end_station_name', 'end_station_description', 'end_station_latitude', 'end_station_longitude']]
            df = df.drop_duplicates(subset=['end_station_id'])
        except:
            print('passed '+ str(date))
            df = pd.DataFrame()
        
        all_stations = pd.concat([all_stations, df])
        all_stations = all_stations.drop_duplicates(subset=['end_station_id'])
            # Create engine for db-connection:
            # engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
            # df.to_sql(tablename, engine, index=False, schema='bysykkel', if_exists='append', method='multi', chunksize=1000)
            # print("Sending data to db!")
            
    return all_stations

def get_month_to_db(date, tablename):
    '''Function to load data from website as json, save it as dataframe,
        and send the dataframe to postgres-db'''
    
    OBS_ROOT= "https://data.urbansharing.com/oslobysykkel.no/trips/v1/"
    resource = requests.get(OBS_ROOT + date)
    try:
        data = resource.json()
        df = pd.DataFrame(data)
        df = df.drop(columns=['end_station_name', 'end_station_description', 'end_station_latitude', 'end_station_longitude', 'start_station_name', 'start_station_description', 'start_station_latitude', 'start_station_longitude', 'duration'])

        # Create engine for db-connection:
        engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
        df.to_sql(tablename, engine, index=False, schema='bysykkel', if_exists='append', method='multi', chunksize=1000)
        print("Sending data to db!")
    except:
        print('passed '+ str(date))
           
    return df

def iter_dates(legacy=False):
    if(legacy==False):
        for year in range(2019, 2021):
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if((year == 2019) & (int(month) < 4)):
                    continue
                if((year == 2020) & (int(month)==11)):
                    break
                yield f'{year}/{month}.json'
    else:
        for year in range(2016, 2019):
            for month in ['03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if(((year == 2016)|(year == 2017)) & (int(month) == 3)):
                    continue
                else:
                    yield f'{year}/{month}.csv.zip'

def legacy_ny_table_insert():
        
    # OBS_ROOT= "https://data-legacy.urbansharing.com/legacy_new_station_id_mapping.csv"
    # resource = requests.get(OBS_ROOT)
    # # data = pd.read_csv(resource.content)
    # parse = str(resource.content).split('\\n')
    # data = pd.DataFrame()
    # rows = []
    # for row in parse[1:-1]:
    #     rows.append(row.split(','))
    # data = pd.DataFrame(rows, columns=['new', 'legacy'])
    # # Create engine for db-connection:
    engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')

    data = pd.read_csv('reddays.csv')
    data.to_sql('reddays', engine, index=False, schema='bysykkel', if_exists='replace', method='multi', chunksize=1000)

    data = pd.read_csv('strikes.csv')
    data.to_sql('strikes', engine, index=False, schema='bysykkel', if_exists='replace', method='multi', chunksize=1000)

    data = pd.read_csv('elevation.csv')
    data.to_sql('elevation', engine, index=False, schema='bysykkel', if_exists='replace', method='multi', chunksize=1000)

    print("Sending data to db!")
    return 1



time.sleep(60)
engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
engine.execute(CreateSchema('bysykkel'))

# # Inserting non-legacy data into DW
total_start_time = time.time()
for date in iter_dates():
    start_time = time.time()
    get_month_to_db(date, 'obos_data')
    print("--- %s seconds ---" % (time.time() - start_time))
print("--- %s total seconds ---" % (time.time() - total_start_time))

# total_start_time = time.time()
for date in iter_dates(True):
    print(get_legacy_month_to_db(date, 'obos_data_legacy'))
print("--- %s total seconds ---" % (time.time() - total_start_time))


legacy_ny_table_insert()