import logging
import inspect
import pandas as pd
import commonqueries
import alphavantagewrapper
import mongomanager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import copy
import configwrapper

class AlphaVantageUpdater():
	def __init__(self,config_file,proxies=None,timeout=300,max_retries=20,error_codes=range(500,600),internal_error_codes=range(400,500),host='localhost',port=27017,username=None,password=None,dbname='finance',collections=None):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		if proxies is None:
			proxies={}
		if collections is None:
			collections={}
		self.av=alphavantagewrapper.AlphaVantage(api_key=self.config.get_string('ALPHAVANTAGE','api_key'),proxies=proxies,timeout=timeout,max_retries=max_retries,error_codes=error_codes,internal_error_codes=internal_error_codes)
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)

		return
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_prices(self,full_update=False,tickers=None):
	
		last_complete_trading_day=pd.to_datetime(self.cq.get_last_complete_market_day())
		logging.info("last_complete_trading_day:"+str(last_complete_trading_day))
		
		self.mongo.create_index(self.collections['alphavantage_prices'],'cik')
		self.mongo.create_index(self.collections['alphavantage_prices'],'date')
		self.mongo.create_index(self.collections['alphavantage_prices'],'ticker')
		
		if tickers is None:
			tickers=self.cq.existing_tickers()
		for ticker in tickers:
			logging.info(ticker+":updating prices with alphavantage")
			cik=self.cq.ticker2cik(ticker)
			prices=self.mongo.db[self.collections['alphavantage_prices']].find({'ticker':ticker})
			prices=pd.DataFrame(list(prices))
			
			if full_update is True or len(prices)==0 or len(prices)!=len(prices['date'].unique()) or len(prices['cik'].unique())>1 or pd.to_datetime(prices['date'].max())<pd.to_datetime(datetime.now().date()-relativedelta(days=30)):
				logging.info('full update or error found:'+ticker)

				self.mongo.db[self.collections['alphavantage_prices']].remove({'ticker':ticker})
				newprices=self.av.get_pandas_time_series_adjusted_daily(ticker,outputsize='full')
				if newprices is None or len(newprices)==0:
					logging.error("newprices is none, continueing")
					continue
			else:
				if pd.to_datetime(prices['date']).max()==last_complete_trading_day:
					logging.info('ticker is already up to date, continuing:'+ticker)
					continue
				logging.info('last update:'+str(prices['date'].max())+':'+ticker)
				newprices=self.av.get_pandas_time_series_adjusted_daily(ticker,outputsize='compact')
				if newprices is None or len(newprices)==0:
					logging.error("newprices is none, continueing")
					continue
				s=pd.to_datetime(newprices.index)>=pd.to_datetime(prices['date'].max()) #only update the prices we dont have yet
				newprices=newprices[s]
				if any(newprices['split_coefficient']!=1) or any(newprices['dividend_amount']!=0) or any(newprices['close'].astype('float')!=newprices['adjusted_close'].astype('float')):
					logging.info('split or dividend happened or bad adj_close:'+ticker)
					self.mongo.db[self.collections['alphavantage_prices']].remove({'ticker':ticker})
					newprices=self.av.get_pandas_time_series_adjusted_daily(ticker,outputsize='full')					
					if newprices is None:
						logging.error("newprices is none, continueing")
						continue
			if newprices is None or len(newprices)==0:
				logging.error("newprices is none, continueing")
				continue
			
			newprices=newprices[pd.to_datetime(newprices.index)<=pd.to_datetime(last_complete_trading_day)] #only go up to the last trading day, dont do right now...
			newprices['cik']=cik
			newprices['ticker']=ticker
			newprices=newprices.sort_index()
			
			if newprices is None or len(newprices)==0:
				continue
				
			for index,data in newprices.iterrows():
				id=ticker+'_'+str(cik)+"_"+index.strftime('%Y-%m-%d')
				newinfo=copy.deepcopy(data.to_dict())
				newinfo2={}
				for field in list(newinfo.keys()):
					newinfo2[field.replace('.','_').replace(' ','').lower().replace('adjusted','adj').replace('dividend_amount',"ex_dividend").replace('split_coefficient','split_ratio').replace('-','_').replace('splitratio','split_ratio')]=newinfo[field]
					del newinfo[field]
				newinfo=newinfo2
				newinfo['date']=index.strftime('%Y-%m-%d')
				newinfo['_id']=id		
				self.mongo.db[self.collections['alphavantage_prices']].update({"_id":id},newinfo,upsert=True)
			logging.info(ticker+":"+str(len(newprices))+" days updated with alphavantage")
			logging.info('new max date for:'+ticker+' is:'+(newprices.index[-1]).strftime('%Y-%m-%d'))
		return True
if __name__ == "__main__":
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	
	a=AlphaVantageUpdater(config_file='finance_cfg.cfg')
	print a.av.get_pandas_time_series_adjusted_daily(symbol='AAPL')
