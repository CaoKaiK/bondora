import logging
import functions as fcs

# init logging
fcs.custom_logger('main')
logger = logging.getLogger('main') 

userId = 982

publicRaw = fcs.save_publicdataset(userId)
publicClean = fcs.clean_data(publicRaw, mode='train')

clf, auc  = fcs.train_forest(publicClean, Search='off')
fit = fcs.evaluate_default_prob(clf, publicClean)

for userId in range(982, 985):
    logger.info(f'############ {userId} ##############')
    # load user data for today
    userData = fcs.save_investments(userId)
    
    # prepare data for random forest
    userClean = fcs.clean_data(userData, mode='apply')
    
    # apply random forest to user data and fit it to exp default rate
    userData['Prob_fitted'] = fcs.apply_forest(fit, clf, userClean)
    
    # calculate adjusted interest accounting for default rate and tayrs
    userData['adjInt'] = fcs.calculate_adjInt(userData)
    
    #choose items to be sold
    userResult= fcs.pick_items(userData, userId)
    
    # check sec market for ongoing sales
    currentSales = fcs.check_sales(userId)
    
    # adjust gain if criteria is met
    adjustedSales, now = fcs.adjust_gain(currentSales)
    
    # cancel the items that need adjustment
    fcs.cancel_items(userId, adjustedSales, now)
    
    # add new items
    items = userResult[['LoanPartId']].copy()
    addedSales = fcs.add_items(adjustedSales, items)
    
    # sell adjusted and new items
    fcs.sell_items(userId, addedSales)



# shut down logging to release filehandles
logger.info('Finished')
logging.shutdown()

