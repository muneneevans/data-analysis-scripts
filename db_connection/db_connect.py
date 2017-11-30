#%%
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import json
import os
import psycopg2

#%%
fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'db_connection/db_config.json')
with open(filename) as my_file:
    db_conf = json.load(my_file)
print db_conf

#%%
def get_connection():
    
    fileDir = os.path.dirname(os.path.realpath('__file__'))
    ilename = os.path.join(fileDir, 'db_connection/db_config.json')
    with open(filename) as my_file:
        db_conf = json.load(my_file)
    conn_str = "host={} dbname={} user={} password={}".format(
                db_conf['host'], db_conf['database'], 
                db_conf['user'], db_conf['passw'])
    
    # return conn_str
    try:
        conn = psycopg2.connect(conn_str)
        return conn
    except Exception:
        return Exception.message
        
get_connection()

#%%
#return a list of all facilities
def get_facilities():
    conn = get_connection()
    all_facilities = pd.DataFrame()
    for chunk in pd.read_sql('select * from facilities_facility', con=conn, chunksize=100):
        all_facilities = all_facilities.append(chunk)
    
    return all_facilities

get_facilities()


#%%
#GET A LIST 
def get_counties():
    ''' returns a dataframe of all counties '''
    conn = get_connection()
    all_counties = pd.DataFrame()
    for chunk in pd.read_sql('SELECT * FROM common_county', con=conn, chunksize=100):
        all_counties = all_counties.append(chunk)
    
    return all_counties

get_counties()

#%%
#GET A LIST 
def get_county_codes():
    ''' returns a dataframe of all counties '''
    conn = get_connection()
    all_counties = pd.DataFrame()
    for chunk in pd.read_sql('SELECT * FROM common_county', con=conn, chunksize=100):
        all_counties = all_counties.append(chunk)
    
    return all_counties[['name','code','county_sk']]

get_county_codes()


#%%
def get_indicators():
    '''return a dataframe of all indicators'''
    conn = get_connection()
    all_indicators = DataFrame()
    for chunk in pd.read_sql('SELECT * FROM dim_dhis_indicator', con=conn, chunksize=100):
        all_indicators = all_indicators.append(chunk)
    
    return all_indicators


#%%
def county_summary():
    facilities = get_facilities()
    grouped_facilities = facilities[['Beds', 'Cots','Facility Name']].groupby( [mfl_dframe['County']])







#%%
counties = get_county_codes()
facilities = get_facilities()
facilities.dtypes
# counties

#%%
indicators = get_indicators()
indicators[24:24]

#%%
indicators = get_indicators()
# indicators[['indicatorname','numeratorsql']][1:2]

sql = indicators[:2]



#%%
''' returns a dataframe of all counties '''
conn = get_connection()
all_counties = pd.DataFrame()
for chunk in pd.read_sql('SELECT * FROM common_county', con=conn, chunksize=100):
    all_counties = all_counties.append(chunk)

# all_counties[['name','code','county_sk']]
all_counties =  all_counties.set_index('name')
all_counties[['code','county_sk']]
# all_counties[['name','code','county_sk']]

#%%
all_counties.to_dict()

#%%
all_counties.to_json()

