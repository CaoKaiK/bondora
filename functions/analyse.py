# -*- coding: utf-8 -*-
'''Custom function to analyse classification performance'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve, auc
from sklearn.utils.multiclass import unique_labels

plt.ioff()

SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIGGER_SIZE = 12

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

def plot_confusion_matrix(y_true, y_pred, classes,
                          normalize=True,
                          title=None,
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    if not title:
        if normalize:
            title = 'Normalized confusion matrix'
        else:
            title = 'Confusion matrix, without normalization'

    # Compute confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    # Only use the labels that appear in the data
    classes = classes[unique_labels(y_true, y_pred)]
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    fig, ax = plt.subplots()
    im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
    ax.figure.colorbar(im, ax=ax)
    # We want to show all ticks...
    ax.set(xticks=np.arange(cm.shape[1]),
           yticks=np.arange(cm.shape[0]),
           # ... and label them with the respective list entries
           xticklabels=classes, yticklabels=classes,
           title=title,
           ylabel='True label',
           xlabel='Predicted label')

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
             rotation_mode="anchor")

    # Loop over data dimensions and create text annotations.
    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], fmt),
                    ha="center", va="center",
                    color="white" if cm[i, j] > thresh else "black")
    fig.tight_layout()
    return ax

def area_under_roc(X, y, clf, plot='no'):

    # overall accuracy
    acc = clf.score(X,y)
    
    # get roc/auc info
    y_score = clf.predict_proba(X)[:,1]
    fpr = dict()
    tpr = dict()
    fpr, tpr, _ = roc_curve(y, y_score)
    
    roc_auc = dict()
    roc_auc = auc(fpr, tpr)
    
    #make the plot
    fig = plt.figure(figsize=(10,10))
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([-0.05, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.grid(True)
    plt.plot(fpr, tpr, label=f'AUC = {roc_auc:.2f}')        
    plt.legend(loc="lower right", shadow=True, fancybox =True)
    if plot=='yes':
        plt.show()
    plt.savefig('auc.png')
    plt.close(fig)
    
    return roc_auc

def feature_imp(feat_list, clf):

    fig = plt.figure(figsize=(15, 20))
    feat_imp = pd.Series(clf.feature_importances_, index=feat_list)
    feat_imp.nlargest(30).plot(kind='barh')
    plt.savefig('feat_imp.png')
    plt.close(fig)

    fig = plt.figure(figsize=(15, 20))
    feat_imp = pd.Series(clf.feature_importances_, index=feat_list)
    feat_imp.nsmallest(10).plot(kind='barh')
    plt.savefig('feat_imp_lowest.png')
    plt.close(fig)