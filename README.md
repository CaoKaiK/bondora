# Bondora - Apply Machine Learning to P2P

*This program was written as a hobby project to learn some python and apply machine learning to the classification of credits. This was also the first project using an API to exchange data/requests. Feel free to fork the project and modify it.*

The intention of the program is to connect to the P2P plattform www.bondora.com and filter the investment portfolio of your credits by the porbabilty of default and sell those credits that do not meet the criteria.


# Functions
## log
This functions defines a custom logger. Log files will be saved in a log folder.

## api
This collection of functions uses the requests library to define different scenarios of api-calls.

### update credentials
Handles the loading of authorization token and the saving of the last instance an api calls has been made. This point in time is needed to time the rate throttling from the side of bondora.

### handle_request
handles errors codes of the request response

### bondora_request
gives a framework for the different types of requests the bondora api provides. Logging for every request is handled in this function.

### get_balance
GET request - returns Balance

### get_secondarymarket
GET request - returns secondarymarket items
*might need to be called several time to get every item*

### post_sellitems
POST request - posts credits that need to be sold, includes pricing
monitors the response and removes items from a batch of credits that cannot be sold currently. Removed credits get logged.
*might need to be called several times to post every item*

### post_cancelitems
POST request - posts credtis that are to be cancelled from being sold on the secondary market
*might need to be called several times to post every item*

### get_investments
GET request - gets list of investments. save_investments will save the data received.
*might need to be called several times to get every item*

### save_publicdataset
downloads publicdataset from https://www.bondora.com/marketing/media/LoanData.zip , unzipps it and saves it as csv.

## rndforest
Applies a random forest classifier on the prepared loan data and outputs the estimates default probabilty. The training is done on a daily basis using the publicdataset that contains every credit. The data are cleaned and to some extend filtered in the collection of functions called dataprep. Modes allow to switch between a fixed config of the rnd-forest parameters or a search mode where the performance for different super-parameters are evaluated.

## analyse
Analyses the rnd-forest classification performance by calculating the confusion matrix, area under roc-curve (receiver operating characteristic) and the feature importance. Some plots are being saved. Plot between different runs are overwritten.



## REST API
The code uses the API that is provided by Bondora. The documentation can be found under https://api.bondora.com



## Folder: data
Rename or delete the data_example folder and modify the .json file that contains the credentials of the accounts that are to be analysed. **Please do not include your API key in any public repo**. Rename the .json to credentials.json.

After the first initialisation run the folder structure will be automatically configured depending on the amount of accounts. For every account there will be a own folder in which relevant portfolio data of the last run will be saved. There will only be one dataset per day and account. Multiple runs a day will overwrite the files from the same day, which is not a problem.