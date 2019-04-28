# -*- coding: utf-8 -*-
'''Sales Manager - manages sales of credit

current function:
    check_sales
    adjust_gain
    cancel_items
    add_items
    sell_items
    '''

import logging
import datetime as dt
import pandas as pd
from math import ceil

from functions.api import get_secondarymarket, post_cancelitem, post_sellitems

logger = logging.getLogger('main')

##############################################################################
def check_sales(userId):
    '''Check currently active sales'''
    # get current items on sale
    currentSales, maxPage = get_secondarymarket(userId)
    if not(currentSales.empty):
        # filter for relevant info
        currentSales = currentSales.filter(['ListedOnDate',
                                            'LoanPartId',
                                            'DesiredDiscountRate',
                                            'Id'])
        # convert to datetime and drop timezone
        currentSales['ListedOnDate'] = pd.to_datetime(currentSales['ListedOnDate'],utc=True)
        currentSales['ListedOnDate'] = currentSales['ListedOnDate'].dt.tz_localize(None)
        # rename
        currentSales = currentSales.rename(index=str,
                                           columns={'DesiredDiscountRate': 'Gain',
                                                    'Id': 'MarketId',
                                                    'ListedOnDate': 'Date'})
    return currentSales


##############################################################################
def adjust_gain(currentSales):
    '''Adjust remaining sales'''
    
    now = dt.datetime.now()
    if  not(currentSales.empty):
        # define threshold for changing gain
        timeThresh = dt.timedelta(hours=12)

        currentSales['NewGain'] = currentSales['Gain']

        # test all dates and change the gain if threshold is reached
        for index, row in currentSales.iterrows():
            if now - row['Date'] > timeThresh:
                currentSales.at[index,'Date'] = now
                currentSales.at[index,'NewGain'] = max(0, row['Gain']-1)

        # drop unchanged credits
        adjustedSales = currentSales[currentSales.Gain != currentSales.NewGain].copy()
        # save new gain
        adjustedSales['Gain'] = adjustedSales['NewGain']

        adjustedSales = adjustedSales.drop(columns='NewGain')

    else:
        adjustedSales = currentSales

    return adjustedSales, now


##############################################################################
def cancel_items(userId, adjustedSales, now):
    '''cancel all items that were adjusted'''
    if not(adjustedSales.empty):
        reqPosts = ceil(len(adjustedSales)/100)
        
        tmp = adjustedSales['MarketId']
        for noPost in range(1, reqPosts+1):
            items = tmp[(noPost-1)*100:(noPost)*100]
            items = items.to_list()
            # cancel the items
            post_cancelitem(userId, items)
    else:
        logger.info('No items to cancel')



##############################################################################
def add_items(adjustedSales, items):
    '''add items to adjustesSales list and init date and gain'''
    adjustedSales = adjustedSales.copy()
    items['Date'] = dt.datetime.now()
    items['Gain'] = 3

    # append adjustedSales with new items
    addedSales = adjustedSales.append(items, sort=True)
    
    addedSales.Gain = addedSales.Gain.astype(int)
    return addedSales


##############################################################################
def sell_items(userId, addedSales):
    ''' make request and save latest sales'''
    if not(addedSales.empty):
        reqPosts = ceil(len(addedSales)/100)
        addedSales = addedSales[['LoanPartId', 'Gain']]
        for noPost in range(1,reqPosts+1):
            items = addedSales[(noPost-1)*100:(noPost)*100]
            items = items.rename(index=str, columns={'Gain': 'DesiredDiscountRate'})
    
            # format to required format for api
            items = items.to_dict(orient='records')
            post_sellitems(userId, items)
    else:
        logger.info('No items to sell')

