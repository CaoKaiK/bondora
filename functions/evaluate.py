# -*- coding: utf-8 -*-
from data import credentials
import pandas as pd



def pick_items(userData, userId):
    # load user
    user = credentials.get_auth(userId)
    # filter for time
    userData['BiddingStartedOn'] = pd.to_datetime(userData.BiddingStartedOn)
    userData = userData[userData.BiddingStartedOn > user['sell_start']]
    # filter for nextPayment == 1
    userData = userData[userData.NextPaymentNr==1]
    # filter for low adjusted interest
    userData = userData[userData.adjInt < 15]
    
    return userData