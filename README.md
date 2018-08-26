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
[requestswrapper.py](src/requestswrapper.py) - Issues get/post requests to the python requests module.  The wrapper is designed for error logging/reporting and will automatically retry the request.  This wrapper is used by every other class which needs to pull data from the internet.

[robinhoodwrapper.py](src/robinhoodwrapper.py) - The interface with robinhood's API. This class handles trading and execution, portfolio reading, and other robinhood functions.  Inspired by [robinhood-python](https://github.com/mstrum/robinhood-python) and [Robinhood](https://github.com/sanko/Robinhood)

[intriniowrapper.py](src/intriniowrapper.py) -The interface to intrinio's API.  This class pulls the raw data from intrinio.  Only a subset of the intrinio endpoints have been implemented, mostly those associated with the *US Fundamentals and Stock Prices* subscription. 