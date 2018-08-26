import warnings
warnings.filterwarnings("ignore")
import pulldata_alphavantage
import pulldata_intrinio
import pulldata_robinhood
import pulldata_nasdaq
import pulldata_quandl
import pulldata_fred
import logging
import inspect
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import sec4parser
import os
import pulldata_iex

def main(config_file='finance_cfg.cfg'):

	rh=pulldata_robinhood.RobinhoodUpdater(config_file=config_file)
	a=pulldata_alphavantage.AlphaVantageUpdater(config_file=config_file)
	i=pulldata_intrinio.IntrinioUpdater(config_file=config_file)
	iex=pulldata_iex.IexUpdater(config_file=config_file)
	n=pulldata_nasdaq.NasdaqUpdater(config_file=config_file)
	q=pulldata_quandl.QuandlUpdater(config_file=config_file)
	f=pulldata_fred.FredUpdater(config_file=config_file)
	
	i.cq.mongo.reindex_collections()
	
	f.update_many_fred_timeseries_data(list(set(['DDDM01USA156NWDB','GDP','WILL5000PRFC','RU3000TR','SP500','CPROFIT','DBAA','DAAA','T10Y3M','DGS3MO','DGS10','DGS1','DGS5','DGS2','USRECD'])))
	f.update_many_fred_timeseries_data(["DSPIC96","RRSFS","PAYEMS","INDPRO"])
	f.update_many_fred_timeseries_data(["DTB1YR", "DTB3",'DGS10','DGS1'])

	q.update_quandl_timeseries_data("FRED","INDPRO")
	q.update_quandl_timeseries_data("FRED","PAYEMS")
	q.update_quandl_timeseries_data("FRED","RRSFS")
	q.update_quandl_timeseries_data("FRED","DSPIC96")
	q.update_quandl_timeseries_data("USTREASURY","YIELD")
	
	#symbols that are valid to trade in robinhood
	rh.update_robinhood_instruments() #updates all robinhood instruments
	instruments=rh.cq.get_robinhood_instruments()
	instruments=instruments[instruments['tradeable']==True]
	valid_instrument_symbols=instruments['symbol'].unique()
	
	i.update_exchanges()
	i.update_securities(exch_symbols=['^XNAS','^XNYS'])
	i.update_standardized_tags_and_labels()
	i.update_companies()
	i.update_all_company_filings()
	i.update_index_prices('$SPX')
	i.update_figi_prices(figi='BBG000BDTBL9') #COMPOSITE FIGI #updating SPY, this is the FIGI for SPY
	i.update_figi_prices(figi='BBG000BDTF76') #REAL FIGI #updating SPY, this is the FIGI for SPY, doing both just because...its only 1 extra call
	i.update_exchange_prices(exch_symbols=['^XNAS','^XNYS'])
	logging.info('now doing the form 4s')
	sec4parser.allfilings_2_form4(i.cq.collections,i.cq.mongo) #update the form 4s
	sec4parser.update_data(i.cq.collections,i.cq.mongo)
	logging.info('finished with the form 4s')
	#
	n.update_nasdaq_companies() #update all nasdaq company information
	companies=i.cq.get_companies()
	companies=companies[pd.to_datetime(companies['latest_filing_date']).dt.date>=datetime.now().date()-relativedelta(months=6)]
	companies=companies[companies['ticker'].isin(valid_instrument_symbols)]
	companies=companies[companies['standardized_active']==True]
	totallen=float(len(companies))
	k=0
	for index,company in companies.iterrows():
		k+=1
		cik=company['cik']
		ticker=company['ticker']
		logging.info('updating:'+cik+" percent complete:"+str(float(k)/totallen))
		n.update_shortinterest(tickers=[ticker])
		rh.update_earnings(tickers=[ticker])
		i.update_standardized_fundamentals(ciks=[cik])
		i.update_standardized_financials(ciks=[cik])

	iex.update_iex_symbols()
	iex.update_iex_stats()

	i.cq.mongo.reindex_collections()
	return
if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	main(config_file='finance_cfg.cfg')