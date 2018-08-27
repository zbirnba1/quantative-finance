# quantative-finance
Trade equities with python algorithms.

# Legal Disclaimer
- The code in this repository may have bugs which could result in real financial consequences.  Please inspect the code prior to execution as you are responsible for anything you execute.
- Robinhood is the assumed broker for execution.  Their API is currently not officially documented or supported.  Several times in the past they have modified the use of the api and as a result, this code stopped working for some time, such an even may happen again.
- You are not to charge or many any money through advertisements or fees using this code.  I am not responsible if Robinhood cancels your account because of misuse of this library.
- I am not affiliated with robinhood, intrininio, iex, nasdaq, quandl, or any of the companies used in the code.  Please direct support questions regarding their products to them.

# Summary
The purpose of this library is to gather and store the necessary financial data to enable the use of algorithmic traders.  The following data sources are currently implemented and stored to varrying degrees:
- [intrinio](https://intrinio.com/) -fundamental data & price history
- [iex](https://iextrading.com/) -fundamental data
- [nasdaq](https://www.nasdaq.com/) -company information & short interest ratios
- [robinhood](https://robinhood.com/) -earnings releases
- [quandl](https://www.quandl.com/) -macroeconomic data
- [federal reserve economic data](https://fred.stlouisfed.org/) -macroeconomic data

Once the data has been stored, it is then processed to determine optimal porfolios for investment per the algorithm selected.  Currently the only algorithm in use is the [quantative value](https://www.amazon.com/Quantitative-Value-Web-Site-Practitioners/dp/1118328078) method created by Dr. Wesley Gray of [alpha architect](https://alphaarchitect.com/) and Tobias Carlisle of [carbon beach](http://carbonbeacham.com).

[recommended_portfolio]:https://docs.google.com/spreadsheets/d/e/2PACX-1vT58ZDK65rGm5_jdGMAPKn1WDUdL27H4jYTyJUl9t_5WKmPvdadHyA7luFbnpf_ljTzneSax6lHtJpG/pubhtml?gid=766300694&single=true
You can see the current [recommended_portfolio][recommended_portfolio] using the quantative value algorithm.

Once the portfolio has been generated it can then be connected to any broker with an api.  Currently only the robinhood broker has been integrated.

In general, the flow of information looks like this:
![drawing](https://docs.google.com/drawings/d/1lDbMzVsnxiupsyEsrTXJx_VcnjeB-13cCeJVT6WFQCs/export/png)

# Code Description
## Wrappers
- [requestswrapper.py](src/requestswrapper.py) - Issues get/post requests to the python requests module.  The wrapper is designed for error logging/reporting and will automatically retry the request.  This wrapper is used by every other class which needs to pull data from the internet.  
- [robinhoodwrapper.py](src/robinhoodwrapper.py) - The interface with robinhood's API. This class handles trading and execution, portfolio reading, and other robinhood functions.  Inspired by [robinhood-python](https://github.com/mstrum/robinhood-python) and [Robinhood](https://github.com/sanko/Robinhood)  
- [intriniowrapper.py](src/intriniowrapper.py) -The interface to intrinio's API.  This is a paid subscription and requires authentication.  This class pulls the raw data from intrinio.  Only a subset of the intrinio endpoints have been implemented, mostly those associated with the *US Fundamentals and Stock Prices* subscription.  
- [iexwrapper.py](src/iexwrapper.py) -The interface to IEX's API.  IEX may require a user token, however you can register for one for free.  The only data which it can pull as of 8/26/18 is the stats endoint as any other data is superseded by the intrinio data.  More endpoints will be added as required by the algorithms.  
- [nasdaqwrapper.py](src/nasdaqwrapper.py) -The interface to the nasdaq webpage.  This does not require an API and parses the html from page queries.  As of 8/26/18 the use of nasdaq data is **NOT** required for the algorithm to run as the shortInterest data is now captured by the IEX wrapper.  
- [quandlwrapper.py](src/quandlwrapper.py) -The interface to quandl data.  Certain endpoints require a free API token.  This wrapper is not used by the quantative value algorithm.  
- [fredwrapper.py](src/fredwrapper.py) -The interface to federal reserve macroeconomic data.  Use required an API which is [free](https://research.stlouisfed.org/docs/api/api_key.html) to obtain.  
- [configwrapper.py](src/configwrapper.py) -Small class which wraps configparser and is used for reading the configuration file.  
- [alphavantagewrapper.py](src/alphavantagewrapper.py) -Interface to [alphavantage](https://www.alphavantage.co/).  Provides technical indicators and historical price data.  This wrapper is not used by the quantative value algorithm. 

## Data Updaters
Classes to take data from the remote sources (using the wrappers) and place it in a mongodb.  The pulldata_*vendor* classes are "intelligent", meaning they will only update what is required, limiting the number of external API calls which are issued.   

- [pulldata_robinhood.py](src/pulldata_robinhood.py).  
- [pulldata_alphavantage.py](src/pulldata_alphavantage.py)  
- [pulldata_quandl.py](src/pulldata_quandl.py)  
- [pulldata_nasdaq.py](src/pulldata_nasdaq.py)  
- [pulldata_intrinio.py](src/pulldata_intrinio.py)  
- [pulldata_iex.py](src/pulldata_iex.py)  
- [pulldata_fred.py](src/pulldata_fred.py)  
- [pulldata.py](src/pulldata.py) - Script with a *main* method which calls each of the above pulldata_*vendor* methods to update all database data.  For optimal perfomance, this should be called once per trading day, after market close.

## Database Connectors
- [mongomanager.py](src/mongomanager.py) - Class which connects to an existing mongo database.  Authentication is strongly recommended.  This class is heavily used by the rest of the code base.

## Data Processing
- [sec4parser.py](src/sec4parser.py) - This script contains methods which will parse through SEC form 4 (insider transcations) filing data.  
- [commonqueries.py](src/commonqueries.py) - This class is used to issue some common queries to the database, such as returning a list of all companies, or returning a pandas DataFrame of a company's fundamentals.  
- [quantvaluedata.py](src/quantvaluedata.py) - This class is used to get specific fundamental values for a specific company.  All methods use either a historical prices DataFrame and/or a 10-K/10-Q statements DataFrame.  Both the prices and statements DataFrames can be queried from commonqueries.py.  
- [metrics.py](src/metrics.py) - This class is used to go through each company and collect the necessary data for it, for example, 5 yr RoA, 1 yr P/E ratio, etc.  This class calls quantvaluedata and commonqueries directly.  
- [recommended_portfolios.py](src/recommended_portfolios.py) - This class is generally called after each company has updated metrics.  Each algorithm has its own method in the class which will return the recommended porfolio for the specified algorithm.  Currently only the quantative value algorithm is implemented.

## Trading
- [robinhoodtransfer.py](src/robinhoodtransfer.py) - This class transfers funds in robinhood from a bank account to the brokerage.  It is not required to run this, it can be usefull with small recurring transfers.  
- [traderobinhood.py](src/traderobinhood.py) - This class executes trades in robinhood.  It tries to be tax efficent, and attempts to always match your current portfolio to the recommended portfolio, as returned by recommended_portfolios.py.  

## Performance Tracking
- [googlesheetuploader.py](src/googlesheetuploader.py) - Contains methods to upload pandas dataframes to google sheets.  Use of this code requies a google cloud service file.  Additional directions to create this file will be added at a later date.  
- [performance.py](src/performance.py) - Will upload performance information, macroeconomic information, recommended portfolio information, and current holdings to a google sheet.

## Running the Pipeline
- [main.py](src/main.py) -The whole pipeline (ccllecting data, processing data, trading, performance tracking) has been implemented inside the main method inside the main.py file.  Just specify a config file to use.

# Configuration File
A configuration file is required to run this code, an example is as follows:

```apacheconf
#financial data is stored in a mongo database, the code is hardcoded to use a dictionary which must have the following collections defined. 
[FINANCIALDATA_COLLECTIONS]
iex_symbols=iex_symbols
iex_stats=iex_stats
intrinio_companies=intrinio_companies
intrinio_filings=intrinio_filings
intrinio_standardized_fundamentals=intrinio_standardized_fundamentals
intrinio_standardized_financials=intrinio_standardized_financials
intrinio_standardized_tags_and_labels=intrinio_standardized_tags_and_labels
intrinio_historical_data=intrinio_historical_data
intrinio_prices=intrinio_prices
alphavantage_prices=alphavantage_prices
robinhood_earnings=robinhood_earnings
nasdaq_short_interest=nasdaq_short_interest
nasdaq_companies=nasdaq_companies
fred_series_observations=fred_series_observations
intrinio_pull_times=pull_times
intrinio_standardized_fundamentals_bad_pull_statements=intrinio_standardized_fundamentals_bad_pull_statements
quandl_timeseries=quandl_timeseries
robinhood_instruments=robinhood_instruments
intrinio_bad_figis=intrinio_bad_figis
intrinio_exchanges=intrinio_exchanges
intrinio_securities=intrinio_securities
sec_form4_xmls=sec_form4_xmls
metrics=metrics
quantative_value_recommended=qvdf

#Fill in the information to access the mongo database.
#if there is no uername/password then comment out those lines
[FINANCIALDATA_MONGO]
host=
username=
password=
dbname=quant_finance
port=27017

#If you have at least one user, enter the collection for the users here
#[USERS_COLLECTIONS]
#robinhood_users=robinhood_users

#It may be wise to store the users in a seperate database, with seperate authentication.
#[USERS_MONGO]
#host=
#username=
#password=
#dbname=quant_finance_users
#port=27017

#[IEX]
#token=

#[ALPHAVANTAGE]
#api_key=

#[QUANDL]
#api_key=

#[INTRINIO]
#username=
#password=

#[GOOGLE_CLOUD]
#service_file=

#if FALSE is specified here will override anything that the user specified in their own options document
#[TRADING]
#trade_options=True #specify
#live_trade=True

```  

# User Document Example
In order to execute trades with robinhood, and view the performance using google docs, a user specific mongodb document is required.

```json
{
    "_id" : "<robinhood_username>",
    "username" : "<robinhood_username>",
    "googlesheetid" : "<google_sheet_id>", //the id of the google sheet to use for performance, it does not need to be populated prior to the first run
    "transfer" : {
        "amount" : 50, //dollars to transfer
        "frequency" : "daily", //how often to transfer
        "id" : "<ach_transfer_id>", //the id of the ach transfer to use, typically a bank account.
        "frequency_multiple" : 1, //how often to transfer
        "last_transfer_id" : "<last_robinhood_transfer>" //this does not need to be populated prior to the first run
    },
    "trade" : {
        "options_trade" : true, //use options in trading
        "live_trade" : true //actually trade stocks with real money
    },
    "password" : "<robinhood_password>",
    "email" : "<email>" //use a gmail address
}
```