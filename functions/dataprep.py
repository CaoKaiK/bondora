# -*- coding: utf-8 -*-
'''prepare data for random forest classifier'''

import logging
import os
import datetime as dt
import pandas as pd

#from sklearn.preprocessing import OneHotEncoder

logger = logging.getLogger('main')

def load_data(fileDir='general'):
    # get today and create path for saving investment list
    dirName = os.path.join('data', fileDir)
    
    today = dt.datetime.now()
    todayStr = today.strftime('%Y_%m_%d')
    filename = f'{todayStr}.csv'
    filepath = os.path.join(dirName, filename)
    
    # load raw data
    dataRaw = pd.read_csv(filepath, low_memory=False)
    length = len(dataRaw)/1000
    logger.info(f'Loaded Data: {length:.1f}k credits')
    return dataRaw


##############################################################################
def clean_data(dataRaw, mode='train'):
    if mode != 'train' or mode != 'apply':
        pass

    if mode == 'train':
        # create label
        if True:
            dataRaw.loc[dataRaw.WorseLateCategory.isnull(), 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '1-7', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '8-15', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '16-30', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '31-60', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '61-90', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '91-120', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '121-150', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '151-180', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.WorseLateCategory == '180+', 'Defaulted'] = 1
            
            #dataRaw.loc[dataRaw.Status == 'Repaid', 'Defaulted'] = 0
            
        else:
            dataRaw.loc[dataRaw.Status == 'Late', 'Defaulted'] = 1
            dataRaw.loc[dataRaw.Status == 'Current', 'Defaulted'] = 0
            dataRaw.loc[dataRaw.Status == 'Repaid', 'Defaulted'] = 0

    # define features
    columnsFeatures = ['BiddingStartedOn',  # will be removed                      
                        'Age',  # continuous
                        'Amount', # continuous
                        'AmountOfPreviousLoansBeforeLoan', # continuous
                        #'ApplicationSignedHour', # 0-23
                        #'ApplicationSignedWeekday', # 1-7
                        'AppliedAmount', # continuous
                        'BidsApi', # continuous
                        'BidsManual', # continuous
                        'BidsPortfolioManager', # continuous
                        'Country', #
                        'Education', #
                        #'EmploymentDurationCurrentEmployer', #
                        'ExistingLiabilities', # continuous
                        'Gender', # 0, 1, 2
                        #'HomeOwnershipType', #
                        'IncomeTotal', # continuous
                        'Interest', # continuous
                        #'LanguageCode', # 1:26
                        'LiabilitiesTotal', # continuous
                        'LoanDuration', # continuous
                        'MonthlyPayment', # continuous
                        #'MonthlyPaymentDay', 
                        'NewCreditCustomer', # True False
                        'NoOfPreviousLoansBeforeLoan', # continuous
                        'PreviousRepaymentsBeforeLoan', # continuous
                        'ProbabilityOfDefault', # continuous
                        'Rating',
                        'VerificationType'
                        ]
    
    if mode == 'train':
        columnsFeatures.append('Defaulted')
    
    # extract features from raw
    dataPresorted = dataRaw[columnsFeatures].copy()
    
    # get row count before cleaning 
    presortedLen = len(dataPresorted.index)
    # format to datetime and sort
    dataPresorted.loc[:, 'BiddingStartedOn'] = pd.to_datetime(dataPresorted.BiddingStartedOn).copy()
    dataPresorted = dataPresorted.sort_values(['BiddingStartedOn'], ascending=True)
    
    
    if mode == 'train':
        # remove data prior three months from today
        today = dt.datetime.now()
        dataSorted = dataPresorted.loc[dataPresorted['BiddingStartedOn'] <= (today - dt.timedelta(days=90)),:].copy()
        sortedLen = len(dataSorted.index)
        removed = (presortedLen - sortedLen) / presortedLen *100
        logger.info(f'{removed:.2f} % time filtered')
    else:
        dataSorted = dataPresorted
        sortedLen = len(dataSorted.index)
        removed = 0

    # additional features
    dataSorted.loc[:, 'AppliedRatio'] = dataSorted.Amount.div(dataSorted.AppliedAmount)
    
    dataSorted.loc[:, 'IncomeCreditRatio'] = dataSorted.MonthlyPayment.div(dataSorted.IncomeTotal)
    dataSorted.loc[dataSorted.IncomeCreditRatio > 2, 'IncomeCreditRatio'] = 2
    

    # replace NaN element with standard values
    values = {'AmountOfPreviousLoansBeforeLoan': 0,
              'Education': 0,
              'Gender': 2,
              'HomeOwnershipType': 10,
              'EmploymentDurationCurrentEmployer': 'Other',
              'MonthlyPayment': 0,
              'NoOfPreviousLoansBeforeLoan': 0,
              'PreviousRepaymentsBeforeLoan': 0,
              'VerificationType': 0
              }
    dataClean = dataSorted.fillna(values)
    
    
    # remove remaining nan
    dataClean = dataClean.dropna(axis=0)
    
    cleanLen = len(dataClean.index)
    removed = (presortedLen - cleanLen) / presortedLen *100 - removed
    if mode =='train':
        logger.info(f'{removed:.2f} % NaN filtered')
    elif mode == 'apply' and removed > 0:
        logger.warning(f'Some credits were removed due to NaN values')

    # one hot encoding
    # define possible categories
    hours = list(range(0,24))
    weekday = list(range(1,8))
    country = ['EE', 'ES', 'FI', 'SK']
    education = list(range(-1,6))
    employment = ['MoreThan5Years', 'UpTo3Years',
                  'UpTo5Years', 'UpTo1Year',
                  'UpTo2Years', 'UpTo4Years',
                  'TrialPeriod', 'Retiree', 'Other']
    gender = list(range(0,3))
    home = list(range(0,11))
    language = list(range(1,27))
    new = [False, True]
    rating = ['AA', 'A', 'B', 'C', 'D', 'E', 'F', 'HR']
    veri = list(range(0,5))
    
    dummy = dataClean.head(30).copy().reset_index(drop=True)
    dummy['Age'] = -99

    for ii in range(0,30):
        dummy.loc[ii, 'VerificationType'] = veri[min(ii,len(veri)-1)]
        dummy.loc[ii, 'Gender'] = gender[min(ii,len(gender)-1)]
        #dummy.loc[ii, 'ApplicationSignedHour'] = hours[min(ii,len(hours)-1)]
        #dummy.loc[ii, 'ApplicationSignedWeekday'] = weekday[min(ii,len(weekday)-1)]
        dummy.loc[ii, 'Country'] = country[min(ii,len(country)-1)]
        dummy.loc[ii, 'Education'] = education[min(ii,len(education)-1)]
        #dummy.loc[ii, 'EmploymentDurationCurrentEmployer'] = employment[min(ii,len(employment)-1)]
        #dummy.loc[ii, 'HomeOwnershipType'] = home[min(ii,len(home)-1)]
        #dummy.loc[ii, 'LanguageCode'] = language[min(ii,len(language)-1)]
        dummy.loc[ii, 'NewCreditCustomer'] = new[min(ii,len(new)-1)]
        dummy.loc[ii, 'Rating'] = rating[min(ii,len(rating)-1)]
    
    dataClean = dataClean.append(dummy, sort=False)
    
    # change specific columns numeric to string to trigger dummy creation
    dataClean['VerificationType'] = dataClean.VerificationType.astype(int).astype(str)
    dataClean['Gender']  = dataClean.Gender.astype(int).astype(str)
    #dataClean['ApplicationSignedHour'] = dataClean.ApplicationSignedHour.astype(int).astype(str)
    #dataClean['ApplicationSignedWeekday'] = dataClean.ApplicationSignedWeekday.astype(int).astype(str)
    dataClean['Education'] = dataClean.Education.astype(int).astype(str)
    #dataClean['HomeOwnershipType'] = dataClean.HomeOwnershipType.astype(int).astype(str)
    #dataClean['LanguageCode'] = dataClean.LanguageCode.astype(int).astype(str)

    dataClean = pd.get_dummies(dataClean)

    # remove dummy rows
    dataClean = dataClean[dataClean.Age != -99]
    
    
    sumNan = sum(dataClean.isna().sum())
    logger.debug(f'Number of remaining NaN: {sumNan}')
    
    if mode == 'train':
        datasetDefaultRate = sum(dataClean['Defaulted']) / len(dataClean['Defaulted'])*100
        logger.info(f'{datasetDefaultRate:.2f} % of credits defaulted')
    
    # drop datetime
    dataClean = dataClean.drop(columns=['BiddingStartedOn'])
    
    logger.info('Finished Data Cleansing')
    
    return dataClean