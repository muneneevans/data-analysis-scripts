#%%
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import json

#%%
config = pd.read_json('./db_config.json')


#%%
with open('db_config.json') as my_file:
    db_conf = json.load(my_file)

#%%
l = DataFrame()
l
