#%%
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import json
import os
import psycopg2


#%%
def read_file(file_name):
    fileDir = os.path.dirname(os.path.realpath('__file__'))
    filename = os.path.join(fileDir, 'backups/'+file_name)
    with open(filename) as my_file:
        db_conf = json.load(my_file)
    return db_conf

#%%
def get_counties():
    #counties table analysis
    json_data = read_file('common_county')
    counties_dataframe = DataFrame(json_data['data']).set_index('index')

    counties_dataframe = counties_dataframe[['name','id']]
    return counties_dataframe.dtypes
    # counties_dataframe[['dataelementname','description']]

get_counties()


#%%
l= [1,2,43,5]
l

