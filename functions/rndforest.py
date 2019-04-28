# -*- coding: utf-8 -*-
import logging
import numpy as np
import pandas as pd
from statistics import stdev

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import RandomizedSearchCV

from sklearn.metrics import roc_curve, auc, r2_score

from functions.analyse import plot_confusion_matrix, area_under_roc, feature_imp

logger = logging.getLogger('main')


def train_forest(dataClean, Search='off'):
    X, y, features = get_labels(dataClean)

    if Search=='on':
        # define search grid
        n_estimators = [int(x) for x in np.linspace(100, 300, num=10)]
        max_features = ['auto', 'sqrt']
        max_depth = [int(x) for x in np.linspace(10, 100, num=11)]
        max_depth.append(None)
        min_samples_split = [500, 1000, 2000, 5000]
        min_samples_leaf = [200, 500 , 1000, 2500]
        bootstrap = ['True', 'False']

        random_grid = {'n_estimators': n_estimators,
                       'max_features': max_features,
                       'max_depth': max_depth,
                       'min_samples_split': min_samples_split,
                       'min_samples_leaf': min_samples_leaf,
                       'bootstrap': bootstrap}

        clf = RandomForestClassifier()
        clf_random= RandomizedSearchCV(estimator=clf,
                                       param_distributions=random_grid,
                                       n_iter=150, cv=3, verbose=5,
                                       random_state=42,
                                       n_jobs=6)
        clf_random.fit(X,y)

    # define randomforest regressor
    clf = RandomForestClassifier(n_estimators=166,
                                 max_depth=30,
                                 max_features='sqrt',
                                 max_leaf_nodes=None,
                                 min_samples_leaf=2500,
                                 min_samples_split=1000,
                                 verbose=0,
                                 n_jobs=6, random_state=42)
    
    if 'clf_random' in locals():
        gridParams = clf_random.best_params_
        clf.set_params(**gridParams)
    
    params = clf.get_params()
    
    logger.debug('Train Random Forest with:')
    logger.debug(f"No of Trees: {params['n_estimators']}")
    logger.debug(f"Max Depth: {params['max_depth']}")
    logger.debug(f"Min Leaf Size: {params['min_samples_leaf']}")
    logger.debug(f"Min Sample Split: {params['min_samples_split']}")
    logger.debug(f"Crit: {params['criterion']}")
    
    
    n = 10
    cv = StratifiedShuffleSplit(n_splits=n)
    cv.split(X,y)


    aucs = []
    i = 0
    for train, test in cv.split(X, y): 
        probas_ = clf.fit(X[train], y[train]).predict_proba(X[test])
        # Compute ROC curve and area the curve
        fpr, tpr, thresholds = roc_curve(y[test], probas_[:, 1])

        roc_auc = auc(fpr, tpr)
        logger.info(f'Shuffle: {i+1} of {n} | AUC: {roc_auc:.3f}')
        aucs.append(roc_auc)

        i += 1
    
    area_under_roc(X, y, clf, plot='no')
    feature_imp(features, clf)
    
    avg_auc = np.mean(aucs)
    stdev_auc = stdev(aucs)
    logger.info(f'Finished | Mean AUC: {avg_auc:.3f} | Stdev: {stdev_auc:.4f}')
    
    return clf, avg_auc

def evaluate_default_prob(clf, dataClean):
    '''evaluate the true default rate with the predicted probability'''
    X, y, features = get_labels(dataClean)
    compare = pd.DataFrame(clf.predict_proba(X)[:,1], columns={'Prob'})
    compare['Prob_bin'] = compare['Prob'].multiply(2.5).round(1).div(2.5)
    compare['True'] = y
    binCount = compare.groupby(['Prob_bin']).count()
    # find bins with low percentage of data
    binCount['perc'] = binCount['True'].div(sum(binCount['True'])).multiply(100)
    min_size = 100/len(binCount)/3
    
    result = compare.groupby(['Prob_bin']).mean()
    # remove bins with low percentage of data
    result = result.loc[binCount['perc']>min_size,:]
    dif = len(binCount) - len(result)
    if dif > 0:
        logger.info(f'{dif} of {len(binCount)} bins were removed')
    if dif/len(binCount) > 0.25:
        logger.warning(f'Number of bins removed above 25%')
    
    fit = np.polyfit(result['Prob'], result['True'], 1)
    
    result['Fitted'] = np.polyval(fit, result['Prob'])
    r2 = r2_score(result['True'].values, result['Fitted'].values)
    logger.info(f'Fit succeded | R2: {r2:.3f}')
    return fit

def apply_forest(fit, clf, userData):
    '''apply random forest model and fit to default rate'''
    X = userData
    y_pred = clf.predict_proba(X)[:,1]
    y_fitted = np.polyval(fit,y_pred)
    return y_fitted

def calculate_adjInt(userCalc):
    userCalc['effInt'] = (1 + userCalc['Interest'].div(100).div(12))**12 - 1
    userCalc['adjInt'] = ((1 - userCalc['Prob_fitted'])
                            * (userCalc['effInt'] + 1)
                            - (0.25 * (1-userCalc['Prob_fitted']) * userCalc['effInt'])
                            - 1) * 100
    return userCalc['adjInt']
    
def get_labels(dataClean):
    # label
    y = np.array(dataClean['Defaulted'])
    # drop label
    X = dataClean.drop('Defaulted', axis=1)
    # save feature names
    feature_list = list(X.columns)
    X = np.array(X)
    return X, y, feature_list