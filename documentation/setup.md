# Setup
Use the following steps to configure the environment.  A debian linux distribution is assumed.  

## Configure MongoDB.  
* Follow the directions [here](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-debian/) to setup mongodb
* If you choose to enable authentication you can follow this tutorial [here](https://medium.com/@matteocontrini/how-to-setup-auth-in-mongodb-3-0-properly-86b60aeef7e8)
 

## Sign up for data providers
* You will need to create accounts for the following data providers:  
    - [iex](https://iextrading.com/) -fundamental data.  This is the only subscription which requires a payment of $40/month.
    - [intrinio](https://intrinio.com/) -fundamental data & price history
    - [federal reserve economic data](https://fred.stlouisfed.org/) -macroeconomic data

## Create Robinhood Account
If you plan to use this library to trade equities with real money, you will need to connect it to a brokerage.  The only one supported at this time (as of 9/1/18) is [robinhood](https://robinhood.com/).

## Performance Tracking
* Create a new google cloud project [here](https://console.cloud.google.com/)
* Create a service account file, and download it using the guide [here](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)

## Configuration File
* Populate the Configuration File using the example [here](/README.md).  
* Create a entry in the users db for your robinhood user using the example [here](/README.md).
