import inspect
import logging
import nasdaqwrapper
import commonqueries
import mongomanager
import pandas as pd
from dateutil.relativedelta import relativedelta
import configwrapper

class NasdaqUpdater():
	def __init__(self,config_file):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		if collections is None:
			collections={}
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)
		self.collections=collections
		self.n=nasdaqwrapper.NasdaqWrapper()
		self.mongo.create_collections(self.collections.values())
		return
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_nasdaq_companies(self,exchanges=['NASDAQ','NYSE','AMEX']):
		self.mongo.create_index(self.collections['nasdaq_companies'],'exchange')
		self.mongo.create_index(self.collections['nasdaq_companies'],'symbol')
		
		df=self.n.download_exchange_information(exchanges)
		for index,company in df.iterrows():
			data=company.to_dict()
			id='_'.join([data['exchange'],data['symbol']])
			data['_id']=id
			self.mongo.db[self.collections['nasdaq_companies']].update({"_id":id},data,upsert=True)
		return True

	def short_interest_exists(self,ticker):
		#return true if there is something to get for the short interest ratio, false if there was an error or it was blank
		updatedratio=self.n.get_short_ratio(ticker)
		if updatedratio is None or len(updatedratio)==0:
			return False
		else:
			return True
	def update_shortinterest(self,tickers=None,full_update=False):
		self.mongo.create_index(self.collections['nasdaq_short_interest'],'ticker')
		self.mongo.create_index(self.collections['nasdaq_short_interest'],'date')
		self.mongo.create_index(self.collections['nasdaq_short_interest'],'cik')
		
		last_valid_day=self.cq.get_last_complete_market_day()
		
		if tickers is None:
			tickers=self.cq.existing_tickers()
		for ticker in tickers:
			logging.info('updating short interest for:'+ticker)
			cik=self.cq.ticker2cik(ticker)
			shortinterests=self.mongo.db[self.collections['nasdaq_short_interest']].find({'ticker':ticker})
			shortinterests=pd.DataFrame(list(shortinterests))
			if len(shortinterests)!=0 and pd.to_datetime(shortinterests['date']).max()>=pd.to_datetime(last_valid_day)-relativedelta(days=15):
				logging.info('no need to re-pull shortinterests, skipping ticker:'+ticker)
				continue
			
			if (full_update is True) or (len(shortinterests)>0 and len(shortinterests['cik'].unique())>1):
				logging.info('removing existing short interests')
				self.mongo.db[self.collections['nasdaq_short_interest']].remove({'ticker':ticker})
			updatedratio=self.n.get_short_ratio(ticker)
			if updatedratio is None or len(updatedratio)==0:
				continue
			updatedratio['ticker']=ticker
			updatedratio['cik']=cik
			updatedratio.rename(columns={'Settlement Date': 'date','Short Interest':'short_interest','Avg Daily Share Volume':'average_daily_volume','Days To Cover':'days_to_cover'}, inplace=True)
			updatedratio=updatedratio.sort_values('date')
			updatedratio['date']=pd.to_datetime(updatedratio['date'])
			for index,row in updatedratio.iterrows():
				data=row.to_dict()
				data['date']=row['date'].strftime('%Y-%m-%d')
				id='_'.join([str(data[x]) for x in ['ticker','cik','date']])
				data['_id']=id
				self.mongo.db[self.collections['nasdaq_short_interest']].update({"_id":id},data,upsert=True)
			logging.info(ticker+":"+str(len(updatedratio))+" short interest updated")		
		return True
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	n=NasdaqUpdater(config_file='finance_cfg.cfg')
	n.update_nasdaq_companies()
	#n.update_shortinterest(tickers=['CAT','MSFT','AAPL'])