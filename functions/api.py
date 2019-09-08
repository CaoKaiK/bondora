# -*- coding: utf-8 -*-
# pylint: disable=E1101, W1203
"""Makes requests to the Bondora API"""

import logging
import math
import time
import datetime as dt
import os
import json
import zipfile

import pandas as pd
import requests

# default parameters for all requests
TIMEOUT = 30
URLBASE = 'https://api.bondora.com/api/v1/'
PAGESIZE = 10_000
WAIT = 3660

# logging
logger = logging.getLogger('main')

##############################################################################
def update_credentials(mode="load", credentials=[]):
    'load or save credentials'
    # load credentials
    if __name__ == '__main__':
        filepath = os.path.join(os.pardir,'data','credentials.json')
    else:
        filepath = os.path.join('data','credentials.json')

    if mode == 'load':
        with open(filepath, 'r') as f:
            credentials = json.load(f)
        return credentials
    elif mode == 'save':
        with open(filepath, 'w') as f:
            json.dump(credentials, f, indent=4, sort_keys=True)
    return None
    
##############################################################################
def handle_request(r):
    '''evaluate and log response status code'''
    if r.status_code == 200 or r.status_code == 202:
        logger.debug(f'Success - {r.status_code}')
    elif r.status_code == 429:
        logger.warning(f'Too many requests - {r.status_code}')
        # log text for debugging
        logger.debug(f'{r.text}')
    elif r.status_code == 409:
        logger.debug(f'Credit not available - {r.status_code}')
        # log text for debugging
        logger.debug(f'{r.text}')
    else:
        logger.error(f'Some other error- {r.status_code}')
        # log text for debugging
        logger.debug(f'{r.text}')
    return None

##############################################################################
def bondora_request(user_id, req_type, req_name, params=[], wait_time=WAIT):
    '''framework for bondora requests'''
    credentials = update_credentials(mode='load')
    user = credentials[user_id]

    # check when req_name was called the last time
    now = dt.datetime.now()
    try:
        next_request = user[f'{req_name}']
        next_request = dt.datetime.strptime(next_request, user['time_fmt'])
    except:
        next_request = now

    # check throttling for request name
    if now < next_request:
        delta = next_request - now
        delta = delta.seconds
        logger.debug(f"Next {req_type}: {req_name} for {user['name']} in {delta}s")
        while delta > 0:
            dif = min(60, delta)
            time.sleep(dif)
            delta = delta - dif
            logger.debug(f'{delta}s remaining')

    # Authorization
    auth = {'Authorization': f"Bearer {user['token']}"}
    url = URLBASE + req_name    

    logger.debug(f"{req_type}: {req_name} for User: {user['name']}")

    if req_type == 'GET':
        r = requests.get(url,
                         headers=auth,
                         params=params,
                         timeout=TIMEOUT)
   
    elif req_type == 'POST':
        r = requests.post(url,
                          headers=auth,
                          json=params,
                          timeout=TIMEOUT)

    # check request
    handle_request(r)
    
    # prepare next request time
    next_request = now + dt.timedelta(seconds=wait_time)
    next_request = dt.datetime.strftime(next_request, user['time_fmt'])
    request_update = {f'{req_name}': next_request}
    
    # update credentials and save
    credentials[user_id].update(request_update)
    update_credentials(mode='save', credentials=credentials)
    
    return r, credentials

        
##############################################################################
def get_balance(user_id):
    '''GET Request - User Balance'''
    r, credentials = bondora_request(user_id=user_id,
                                     req_type='GET',
                                     req_name='account/balance',
                                     wait_time=360)
    
    # extract payload from json to dict
    payload = json.loads(r.text)['Payload']
    
    return payload

##############################################################################    
def get_secondarymarket(user_id):
    '''GET Request - Secondary market items'''
    
    page_size = 20_000
    page_nr = 1
    
    parameters = {'ShowMyItems': True,
                  'PageSize': page_size,
                  'PageNr': page_nr}

    r, credentials = bondora_request(user_id=user_id,
                                              req_type='GET',
                                              req_name='secondarymarket',
                                              params=parameters,
                                              wait_time=750)
    # extract payload from json to dict
    payload = json.loads(r.text)['Payload']    
    payload = pd.DataFrame.from_dict(payload)
    count = json.loads(r.text)['TotalCount']            
    max_page = math.ceil(count/page_size)
    logger.info(f'Received Secondary Market Page {page_nr} of {max_page}')
    
    return payload, max_page

##############################################################################
def post_sellitems(user_id, items):
    '''POST Request - Sell items on secondary market'''
     
    parameters = {'Items': items,
                  'CancelItemOnPaymentReceived': True,
                  'CancelItemOnReschedule': True}
    
    r, credentials = bondora_request(user_id=user_id,
                                     req_type='POST',
                                     req_name='secondarymarket/sell',
                                     params=parameters,
                                     wait_time=1)
     
    # iterate over batch and remove single items if item can not be sold
    i = 0
    while i < 99 and r.status_code == 409:
        response = json.loads(r.text)
        missing_item = response['Errors'][0]['Details']
        items = pd.DataFrame.from_dict(items)
        new_items = items[items['LoanPartId']!=missing_item]
        items = new_items.to_dict(orient='records')
        
        parameters = {'Items': items,
                      'CancelItemOnPaymentReceived': True,
                      'CancelItemOnReschedule': True}
        
        r, credentials = bondora_request(user_id=user_id,
                                         req_type='POST',
                                         req_name='secondarymarket/sell',
                                         params=parameters,
                                         wait_time=1)
        i += 1
    if i > 0:
        logger.debug(f'{i} credits could not be sold')
    
    if r.status_code == 202:
        logger.info(f'Sold {len(items)}/{len(items)+i} credits')
    else:
        logger.warning(f'Batch was not sold')
        logger.debug(f'{parameters}')
    return None

##############################################################################
def post_cancelitem(user_id, items):
    
    parameters = {"ItemIds": items}
    
    r, credentials = bondora_request(user_id=user_id,
                                      req_type='POST',
                                      req_name='secondarymarket/cancel',
                                      params=parameters,
                                      wait_time=1)
    
    logger.info(f'Canceled {len(items)} credits')
    return None

##############################################################################
def get_investments(user_id, page_number=1):
    '''Gets list of investments for user

    Sales Status
    NULL All active
    Bought investments
    1 Sold investments
    2 Investment is on sale
    3 Investment is not on sale

    '''
    sales_status = 3
    page_size = 50_000
    
    parameters = {'SalesStatus': sales_status,
                  'PageSize': page_size,
                  'PageNr': page_number}
    
    # remove SalesStatus if unused
    if sales_status == 'Null':
        del parameters['SalesStatus']
    
    r, credentials = bondora_request(user_id=user_id,
                                              req_type='GET',
                                              req_name='account/investments',
                                              wait_time=760)
    # extract payload from json to dict
    payload = json.loads(r.text)['Payload']
    payload = pd.DataFrame.from_dict(payload)
    count = json.loads(r.text)['TotalCount']
    max_page = math.ceil(count/page_size)
    logger.info(f'Received Investments Page 1 of {max_page}')
    
    return payload, max_page

##############################################################################
def save_investments(user_id):
    '''Saves current list of investments for user
    investmenst will be saved to data/user'''
    
    credentials = update_credentials(mode='load')
    user = credentials[user_id]
    # define directory
    if __name__ == '__main__':
        dir_name = os.path.join(os.pardir, 'data', user['name'])
    else:
        dir_name = os.path.join('data', user['name'])

    # check if directories exist if not create it
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logger.info(f'{dir_name} was created')

    # get today and create path for saving investment list
    today = dt.datetime.now()
    today = today.strftime('%Y_%m_%d')
    filename = f'{today}.csv'
    filepath = os.path.join(dir_name, filename)

    # check if there is already a dataset for today
#    if os.path.isfile(filepath):
#        logger.info('Loandataset already exists for today')
#        investments = load_data(fileDir=user['name'])
#        return investments

    # get investments list
    investments, max_page = get_investments(user_id, 1)
    

    # get remaining investments pages if maxPage is greater 1
    if max_page > 1:
        for page in range(2, max_page+1):
            next_investments, _ = get_investments(user_id, page)
            investments = pd.concat([investments, next_investments])

    # search for todays public dataset
    if __name__ == '__main__':
        search_file = os.path.join(os.pardir, 'data', 'general', filename)
    else:
        search_file = os.path.join('data', 'general', filename)

    if os.path.isfile(search_file):
        # load dataset
        dataset = pd.read_csv(search_file, low_memory=False)
        # get unique columns from investments
        add_col = investments.columns.difference(dataset.columns)
        add_col = add_col.insert(0, 'LoanId')
        # inner join with public dataset to get additional information
        investments = pd.merge(dataset, investments[add_col],
                               left_on='LoanId',
                               right_on='LoanId',
                               how='inner')
        
        # save dataset to csv file with today as filename
        investments.to_csv(filepath, encoding='utf-8', index=False)
        logger.info(f'Investments saved')

    else:
        logger.warning(f"Today's LoanData not found - Investments not saved")

    return investments
    


##############################################################################
def save_publicdataset():   
    # define saving location
    if __name__ == '__main__':
        dir_name = os.path.join(os.pardir, 'data', 'general')
    else:
        dir_name = os.path.join('data', 'general')
        
    today = dt.datetime.now().strftime('%Y_%m_%d')
    filepath = os.path.join(dir_name, f'{today}.csv')
    
    # check if directories exist if not create it
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        logger.info(f'Directory was created: {dir_name}')

    # check if there is already a dataset for today
    if not os.path.isfile(filepath):
        # define directory and temporary filenames
        dir_name = 'tmp'
        filepath_zip = os.path.join(dir_name, 'LoanData.zip')
        filepath_csv = os.path.join(dir_name, 'LoanData.csv')
        # check if directories exist if not create it
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            logger.info(f'Directory was created: {dir_name}')
        
        logger.info(f'Downloading Dataset')
        url = 'https://www.bondora.com/marketing/media/LoanData.zip'
        r = requests.get(url)
    
        with open('tmp/LoanData.zip', 'wb') as f:  
            f.write(r.content)
        
        with zipfile.ZipFile(filepath_zip, 'r') as zip_ref:
            zip_ref.extractall(dir_name)
        
        data_raw = pd.read_csv(filepath_csv, low_memory=False)
        # clean up csv
        data_raw = data_raw.reindex(sorted(data_raw.columns), axis=1)
        data_raw.LoanId = data_raw.LoanId.str.lower()
        data_raw.BiddingStartedOn = pd.to_datetime(data_raw.BiddingStartedOn).dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        try:
           data_raw = data_raw.rename(index=str,
                                      columns={"PreviousEarlyRepaymentsBefoleLoan": "PreviousEarlyRepaymentsBeforeLoan"})
        except:
            logger.warning('Something changed')
        
        # save dataset to csv file with today as filename
        data_raw.to_csv(filepath, encoding='utf-8', index=False)
        logger.info(f'Public Dataset saved')
    else:
        logger.info('Public Dataset already exists for today')            

    data_raw = pd.read_csv(filepath, low_memory=False)

    return data_raw

##############################################################################

if __name__ == '__main__':
    '''Test functions'''
    print('--- Testing api.py ---')
    data_raw = save_publicdataset()
    # balance
#    r, payload, credentials = bondora_request(user_id=2,
#                                     req_type='GET',
#                                     req_name='account/balance')
#    
#    investments = save_investments(user_id=0)
    
#    parameters = {'SalesStatus': 3,
#                  'PageSize': 20_000,
#                  'PageNr': 1}
#    
#    r1, payload1, credentials = bondora_request(user_id=2,
#                                      req_type='GET',
#                                      req_name='account/investments',
#                                      params=parameters)
    
    page_size = 20_000
    page_nr = 1
    
    parameters = {'ShowMyItems': True,
                  'PageSize': page_size,
                  'PageNr': page_nr}
    
    r, credentials = bondora_request(user_id=0,
                                     req_type='GET',
                                     req_name='secondarymarket',
                                     params=parameters)
    
    
