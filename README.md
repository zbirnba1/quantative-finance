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
[requestswrapper.py](src/requestswrapper.py) - Issues get/post requests to the python requests module.  The wrapper is designed for error logging/reporting and will automatically retry the request.  This wrapper is used by every other class which needs to pull data from the internet.  
[robinhoodwrapper.py](src/robinhoodwrapper.py) - The interface with robinhood's API. This class handles trading and execution, portfolio reading, and other robinhood functions.  Inspired by [robinhood-python](https://github.com/mstrum/robinhood-python) and [Robinhood](https://github.com/sanko/Robinhood)  
[intriniowrapper.py](src/intriniowrapper.py) -The interface to intrinio's API.  This is a paid subscription and requires authentication.  This class pulls the raw data from intrinio.  Only a subset of the intrinio endpoints have been implemented, mostly those associated with the *US Fundamentals and Stock Prices* subscription.  
[iexwrapper.py](src/iexwrapper.py) -The interface to IEX's API.  IEX may require a user token, however you can register for one for free.  The only data which it can pull as of 8/26/18 is the stats endoint as any other data is superseded by the intrinio data.  More endpoints will be added as required by the algorithms.  
[nasdaqwrapper.py](src/nasdaqwrapper.py) -The interface to the nasdaq webpage.  This does not require an API and parses the html from page queries.  As of 8/26/18 the use of nasdaq data is **NOT** required for the algorithm to run as the shortInterest data is now captured by the IEX wrapper.  
[quandlwrapper.py](src/quandlwrapper.py) -The interface to quandl data.  Certain endpoints require a free API token.  This wrapper is not used by the quantative value algorithm.  
[fredwrapper.py](src/fredwrapper.py) -The interface to federal reserve macroeconomic data.  Use required an API which is [free](https://research.stlouisfed.org/docs/api/api_key.html) to obtain.  
[configwrapper.py](src/configwrapper.py) -Small class which wraps configparser and is used for reading the configuration file.  
[alphavantagewrapper.py](src/alphavantagewrapper.py) -Interface to [alphavantage](https://www.alphavantage.co/).  Provides technical indicators and historical price data.  This wrapper is not used by the quantative value algorithm. 

## Data Updaters
Classes to take data from the remote sources (using the wrappers) and place it in a mongodb.  The pulldata_*vendor* classes are "intelligent", meaning they will only update what is required, limiting the number of external API calls which are issued.   

[pulldata_robinhood.py](src/pulldata_robinhood.py).  
[pulldata_alphavantage.py](src/pulldata_alphavantage.py)  
[pulldata_quandl.py](src/pulldata_quandl.py)  
[pulldata_nasdaq.py](src/pulldata_nasdaq.py)  
[pulldata_intrinio.py](src/pulldata_intrinio.py)  
[pulldata_iex.py](src/pulldata_iex.py)  
[pulldata_fred.py](src/pulldata_fred.py)  

[pulldata.py](src/pulldata.py) - Script with a *main* method which calls each of the above pulldata_*vendor* methods to update all database data.  For optimal perfomance, this should be called once per trading day, after market close.

## Database Connectors
[mongomanager.py](src/mongomanager.py) - Class which connects to an existing mongo database.  Authentication is strongly recommended.  This class is heavily used by the rest of the code base.

## Data Processing
[sec4parser.py](src/sec4parser.py) - This script contains methods which will parse through SEC form 4 (insider transcations) filing data.  
[commonqueries.py](src/commonqueries.py) - This class is used to issue some common queries to the database, such as returning a list of all companies, or returning a pandas DataFrame of a company's fundamentals. 
[quantvaluedata.py](src/quantvaluedata.py) - This class is used to get specific fundamental values for a specific company.  All methods use either a historical prices DataFrame and/or a 10-K/10-Q statements DataFrame.  Both the prices and statements DataFrames can be queried from commonqueries.py.  
