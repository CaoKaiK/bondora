import logging
import functions as fcs

from apscheduler.schedulers.blocking import BlockingScheduler



def main():
    # init logging
    fcs.custom_logger('main')
    logger = logging.getLogger('main')

    public_raw = fcs.save_publicdataset()
    public_clean = fcs.clean_data(public_raw, mode='train')

    clf, auc  = fcs.train_forest(public_clean, Search='off')
    fit = fcs.evaluate_default_prob(clf, public_clean)

    for user_id in range(0, 3):
        logger.info(f'############ {user_id} ##############')
        # load user data for today
        user_data = fcs.save_investments(user_id)
        
        # prepare data for random forest
        user_clean = fcs.clean_data(user_data, mode='apply')
        
        # apply random forest to user data and fit it to exp default rate
        user_data['Prob_fitted'] = fcs.apply_forest(fit, clf, user_clean)
        
        # calculate adjusted interest accounting for default rate and tayrs
        user_data['adjInt'] = fcs.calculate_adjInt(user_data)
        
        #choose items to be sold
        user_result= fcs.pick_items(user_data, user_id)
        
        # check sec market for ongoing sales
        current_sales = fcs.check_sales(user_id)
        
        # adjust gain if criteria is met
        adjusted_sales, now = fcs.adjust_gain(current_sales)
        
        # cancel the items that need adjustment
        fcs.cancel_items(user_id, adjusted_sales, now)
        
        # add new items
        items = user_result[['LoanPartId']].copy()
        added_sales = fcs.add_items(adjusted_sales, items)
        
        # sell adjusted and new items
        fcs.sell_items(user_id, added_sales)

    # shut down logging to release filehandles
    logger.info('Finished')
    logging.shutdown()

sched = BlockingScheduler()
sched.add_job(func=main ,trigger='interval', hours=4)