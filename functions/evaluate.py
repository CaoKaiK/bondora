# -*- coding: utf-8 -*-
import datetime as dt
import pandas as pd

from functions.api import update_credentials


def pick_items(user_data, user_id):
    # load user
    credentials = update_credentials(mode='load')
    user = credentials[user_id]
    
    # filter for time
    user_data['BiddingStartedOn'] = pd.to_datetime(user_data.BiddingStartedOn)
    sell_start = dt.datetime.strptime(user['sell_start'], user['time_fmt'])
    user_data = user_data[user_data.BiddingStartedOn > sell_start]
    
    # filter for items on secondary market
    user_data = user_data[user_data['ListedInSecondMarketOn'].isnull()]
    
    # filter for nextPayment == 1
    user_data = user_data[user_data.NextPaymentNr==1]
    
    # filter for low adjusted interest
    user_data = user_data[user_data.adjInt<17.5]
    
    return user_data