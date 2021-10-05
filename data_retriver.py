import pandas as pd
import numpy as np
import os.path
import json
import csv

import requests
from requests_toolbelt import sessions
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import time
from datetime import datetime, timedelta

import threading


def get_data(tf, period, code_id):
    
    # Connect and get the page: 

    session = requests.Session() 
    retry = Retry(total = 30, backoff_factor = 0.5)
    adapter = HTTPAdapter(max_retries = retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    base =  'https://candle.etoro.com'
    url_path = '/'.join(['', 'candles', 'asc.json', tf, str(period), code_id])
    conn =  session.get(base + url_path)
        
    response = conn.content


    # Transform in df:
    
    jsd = json.loads(response)
    df = pd.DataFrame(jsd['Candles'][0]['Candles'])
    
    try:
        if tf != 'OneDay' or tf != 'OneWeek':
            
            df['FromDate'] = df['FromDate'].map(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=2))
        
        else:
            
            df['FromDate'] = df['FromDate'].map(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        
        df = df.iloc[:-1, :]
    
    except:
        
        '''
        This must be improved (maybe a json file?)
        '''
        
        with open("missing_data.txt", "a+") as f:
            check_d = f.read(2)
            if check_d > 0:
                f.write(', ')
                f.write(code_id)
                f.write(' - ' + tf)
            else:
                f.write(code_id)

    return df

def history(code_id, tf, period, path):
    
    # Check path:
    
    title = '_'.join([code_id, tf])
    pt = ''.join([path, title, '.csv'])
    
    if os.path.exists(pt):
        
        ''''It can be implemented better. Maybe using an external DB'''
        
        with open(pt) as f:
            file = f.readlines()
            last_date = datetime.strptime(file[-1].split(',')[1], '%Y-%m-%d %H:%M:%S')

            
            
            
    
    # Get data:
        
    hist = get_data(tf, period, code_id)
    
    # Save as csv

    hist.to_csv(''.join([path, title, '.csv']), index = False)  

def get_instruments_name(path):
    
    base =  sessions.BaseUrlSession(base_url="https://api.etorostatic.com")
    conn =  base.get('/sapi/instrumentsmetadata/V1.1/instruments')
    response = conn.content
    
    # Get the info:
    
    jsd = json.loads(response)
    data = jsd["InstrumentDisplayDatas"]
   
    code_name = {}
    for el in data:
        
        key = el['InstrumentID']
        value = el['SymbolFull']
        code_name[key] = value
    
    # Save as json:
    
    with open(path, 'w') as fjs:
        json.dump(code_name, fjs)

def thread_manager(lst, path, tf, period):
    
    threads = []
    for cid in lst: 
        t = threading.Thread(target = history, args = (cid, tf, period, path))
        t.start()
        threads.append(t)
        
    for process in threads:
        process.join()
