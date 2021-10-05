import pandas as pd
import numpy as np
import json

import os.path

import plotly.graph_objects as go
from plotly.offline import plot

from utils import get_data, history, get_instruments_name, thread_manager

PATH_HIST = 'C:/Users/dario/Google Drive/fin_proj/historical_data/'
PATH_IDS = 'C:/Users/dario/Google Drive/fin_proj/code_name.json'
MAIN_DIR = 'C:/Users/dario/Google Drive/fin_proj/'
     
if __name__ == '__main__':
    
    # Load the dic with the ids and symbols:
    
    if not os.path.exists(PATH_IDS):
        
        get_instruments_name(PATH_IDS)
        
    dic_id = json.load(open(PATH_IDS))
    ids = dic_id.keys()

    # Get historical data

    thread_manager(ids, PATH_HIST, 'OneDay', 1825)
 

ohlc = get_data('OneDay', 1825, '100000')  
ohlc.head()
history('OneHour', 100, '100000', PATH_HIST) 

pd.read_csv(PATH_HIST + '100000_OneHour.csv')

fig = go.Figure(data=[go.Candlestick(x=ohlc['FromDate'],
                open=ohlc['Open'], high=ohlc['High'],
                low=ohlc['Low'], close=ohlc['Close'])
                     ])

fig.update(layout_xaxis_rangeslider_visible=False)
plot(fig)

import csv
from datetime import datetime, timedelta
with open(PATH_HIST + '1_OneDay.csv', 'r') as f:
    file = f.readlines()
    print(datetime.strptime(file[-1].split(',')[1], '%Y-%m-%d %H:%M:%S'))
    datetime.today()
    
pd.read_csv(PATH_HIST + '1_OneDay.csv')