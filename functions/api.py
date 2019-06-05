# -*- coding: utf-8 -*-

# pylint: disable=E1101, W1203
"""Makes requests to the Bondora API

Current functions:
    prep_auth - authorization

    get_balance

    get_secondarymarket

    post_sellitems

    post_cancelitems

    save_investments
        get_investments

    save_publicdataset
        get_publicdataset
"""

import logging
import math
import time
import datetime
import os
import json

import pandas as pd
import requests

from data import credentials
from functions.dataprep import load_data



# Parameter needed for all types of requests
TIMEOUT = 30
URLBASE = 'https://api.bondora.com/api/v1/'
PAGESIZE = 10_000
WAIT = 3610

logger = logging.getLogger('main')

##############################################################################
def prep_auth(userId):
    '''Prepares Authorization Header for request'''
    user = credentials.get_auth(userId)
    token = f"Bearer {user['token']}"
    auth = {'Authorization': token}
    return auth, user


##############################################################################
def get_balance(userId):
    '''GET Request - Account Balance for user_id'''
    auth, user = prep_auth(userId)

    reqName = 'account/balance'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    
    r = requests.get(url, headers=auth, timeout=TIMEOUT)
    time.sleep(WAIT)

    if r.status_code == requests.codes.ok:
        response = json.loads(r.text)
        payload = response['Payload']
        payload = pd.DataFrame.from_dict(payload, orient='index')
        logger.debug(f'Status Code: {r.status_code} - Success')
        logger.info(f'Received Balance')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        payload = pd.DataFrame()
    return payload


##############################################################################
def get_secondarymarket(userId, pageNr=1, showItems=True):
    '''GET Request - Get secondary market items'''
    auth, user = prep_auth(userId)

    parameters = {'ShowMyItems': showItems,
                  'PageSize': PAGESIZE,
                  'PageNr': pageNr}

    reqName = 'secondarymarket'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    logger.debug(f'Show Items: {showItems}')
    
    r = requests.get(url, headers=auth, params=parameters, timeout=TIMEOUT)
    time.sleep(WAIT)

    if r.status_code == requests.codes.ok:
        response = json.loads(r.text)
        totalCount = response['TotalCount']
        maxPage = math.ceil(totalCount/PAGESIZE)
        payload = response['Payload']
        payload = pd.DataFrame.from_dict(payload)
        logger.debug(f'Status Code: {r.status_code} - Success')
        if maxPage > 0:
            logger.info(f'Received Page {pageNr} of {maxPage}')
        else:
            logger.info('No items on secondary market')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        payload = pd.DataFrame()
        maxPage = 0
    return payload, maxPage


##############################################################################
def post_sellitems(userId, items):
    '''POST Request - Sell items on secondary market'''
    auth, user = prep_auth(userId)

    parameters = {'Items': items,
                  'CancelItemOnPaymentReceived': True,
                  'CancelItemOnReschedule': True}

    reqName = 'secondarymarket/sell'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    
    r = requests.post(url, headers=auth, json=parameters, timeout=TIMEOUT)
    time.sleep(WAIT)

    # iterate over batch and remove single items if item can not be sold
    i = 0
    while i < 99 and r.status_code == 409:
        response = json.loads(r.text)
        missingItem = response['Errors'][0]['Details']
        items = pd.DataFrame.from_dict(items)
        newItems = items[items['LoanPartId']!=missingItem]
        items = newItems.to_dict(orient='records')
        
        parameters = {'Items': items,
                  'CancelItemOnPaymentReceived': True,
                  'CancelItemOnReschedule': True}
        
        
        r = requests.post(url, headers=auth, json=parameters, timeout=TIMEOUT)
        time.sleep(WAIT)
        i = i +1

    if i > 0:
        logger.info(f'{i} credits could not be sold')


    if r.status_code == 202: # see api documentation
        response = json.loads(r.text)
        payload = response['Payload']
        payload = pd.DataFrame.from_dict(payload)
        logger.debug(f'Status Code: {r.status_code} - Success')
        logger.info(f'Sold {len(items)} credits')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        response = json.loads(r.text)
        logger.warning(f'{response}')
        payload = pd.DataFrame()
    return payload


##############################################################################
def post_cancelitem(userId, items):
    '''POST Request - Cancel items on secondary market'''
    auth, user = prep_auth(userId)

    parameters = {"ItemIds": items}

    reqName = 'secondarymarket/cancel'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    
    r = requests.post(url, headers=auth, json=parameters, timeout=TIMEOUT)
    time.sleep(WAIT)


    if r.status_code == 202: # see api documentation
        response = json.loads(r.text)

        logger.debug(f'Status Code: {r.status_code} - Success')
        logger.info(f'Canceled {len(items)} credits')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        response = json.loads(r.text)
        logger.warning(f'{response}')


##############################################################################
def get_investments(userId, pageNr=1, salesStatus=3):
    '''Gets list of investments for user

    Sales Status
    NULL All active
    Bought investments
    1 Sold investments
    2 Investment is on sale
    3 Investment is not on sale

    '''
    auth, user = prep_auth(userId)

    parameters = {'SalesStatus': salesStatus,
                  'PageSize': PAGESIZE,
                  'PageNr': pageNr}
    # remove SalesStatus if unused
    if salesStatus == 'Null':
        del parameters['SalesStatus']

    reqName = 'account/investments'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    
    r = requests.get(url, headers=auth, params=parameters, timeout=TIMEOUT)
    time.sleep(WAIT)

    if r.status_code == requests.codes.ok:
        response = json.loads(r.text)
        totalCount = response['TotalCount']
        maxPage = math.ceil(totalCount/PAGESIZE)
        payload = response['Payload']
        payload = pd.DataFrame.from_dict(payload)
        logger.debug(f'Status Code: {r.status_code} - Success')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        payload = pd.DataFrame()
        maxPage = 0
    return payload, maxPage


##############################################################################
def save_investments(userId=984):
    '''Saves current list of investments for user
    investmenst will be saved to data, user'''
    _, user = prep_auth(userId)

    # define directory
    dirName = os.path.join('data', user['name'])

    # check if directories exist if not create it
    if not os.path.exists(dirName):
        logger.info('User Directory was not found')
        os.makedirs(dirName)
        logger.info('User Directory was created')

    # get today and create path for saving investment list
    today = datetime.datetime.now()
    today = today.strftime('%Y_%m_%d')
    filename = f'{today}.csv'
    filepath = os.path.join(dirName, filename)

    # check if there is already a dataset for today
#    if os.path.isfile(filepath):
#        logger.info('Loandataset already exists for today')
#        investments = load_data(fileDir=user['name'])
#        return investments

    # get investments list
    investments, maxPage = get_investments(userId, 1)
    logger.info(f'Received Page 1 of {maxPage}')

    # get remaining investments pages if maxPage is greater 1
    if maxPage > 1:
        for page in range(2, maxPage+1):
            nextInvestments, _ = get_publicdataset(userId, page)
            investments = pd.concat([investments, nextInvestments])
            logger.info(f'Received Page {page} of {maxPage}')

    # search for todays public dataset
    searchFile = os.path.join('data', 'general', filename)
    if os.path.isfile(searchFile):
        # load dataset
        dataset = pd.read_csv(searchFile, low_memory=False)
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
def get_publicdataset(userId, pageNr=1):
    '''Gets daily public dataset for user'''
    auth, user = prep_auth(userId)

    parameters = {'PageSize': PAGESIZE, 'PageNr': pageNr}

    reqName = 'publicdataset'
    url = URLBASE + reqName

    logger.debug(f"Requested for user: {user['name']}")
    
    r = requests.get(url, headers=auth, params=parameters, timeout=TIMEOUT)
    time.sleep(WAIT)

    if r.status_code == requests.codes.ok:
        response = json.loads(r.text)
        totalCount = response['TotalCount']
        maxPage = math.ceil(totalCount/PAGESIZE)
        payload = response['Payload']
        payload = pd.DataFrame.from_dict(payload)
        logger.debug(f'Status Code: {r.status_code} - Success')
    else:
        logger.warning(f'Status Code: {r.status_code} - Failed')
        payload = pd.DataFrame()
        maxPage = 0
    return payload, maxPage


##############################################################################
def save_publicdataset(userId):
    '''Saves daily public dataset
    dataset will be saved to path data, general
    '''

    # define directory and check if exist
    dirName = os.path.join('data', 'general')

    # check if directories exist if not create it
    if not os.path.exists(dirName):
        logger.info('User Directory was not found')
        os.makedirs(dirName)
        logger.info('User Directory was created')

    # get today and create path for saving dataset
    today = datetime.datetime.now()
    today = today.strftime('%Y_%m_%d')
    filename = f'{today}.csv'
    filepath = os.path.join(dirName, filename)

    # check if there is already a dataset for today
    if os.path.isfile(filepath):
        logger.info('Loandataset already exists for today')
        dataset = load_data(fileDir='general')
        return dataset

    # get public dataset
    dataset, maxPage = get_publicdataset(userId, 1)
    logger.info(f'Received Page 1 of {maxPage}')

    # get remaining dataset pages if maxPage is greater 1
    if maxPage > 1:
        for page in range(2, maxPage+1):
            nextDataset, _ = get_publicdataset(userId, page)
            dataset = pd.concat([dataset, nextDataset])
            logger.info(f'Received Page {page} of {maxPage}')

    # save dataset to csv file with today as filename
    dataset.to_csv(filepath, encoding='utf-8', index=False)
    logger.info(f'Public Data set saved')
    return dataset


##############################################################################

def main():
    '''just a docstring'''
    print('Testing api.py')
    balance = get_balance(984)
    print(balance)

if __name__ == '__main__':
    main()
    