import logging
import inspect
import pandas as pd
import commonqueries
import robinhoodwrapper
import mongomanager
import configwrapper
import os

class RobinhoodUpdater():
	def __init__(self,config_file,proxies=None,timeout=300,max_retries=20,error_codes=range(500,600),internal_error_codes=range(400,500),host='localhost',port=27017,username=None,password=None,dbname='finance',collections=None):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections

		if proxies is None:
			proxies={}
		if collections is None:
			collections={}
		self.rh=robinhoodwrapper.RobinHoodWrapper(proxies=proxies,timeout=timeout,max_retries=max_retries,error_codes=error_codes,internal_error_codes=internal_error_codes)
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)

	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_robinhood_instruments(self):
		logging.info('updating robinhood instruments')
		self.mongo.create_index(self.collections['robinhood_instruments'],'url')
		self.mongo.create_index(self.collections['robinhood_instruments'],'symbol')
		self.mongo.create_index(self.collections['robinhood_instruments'],'instrument')

		existing_urls=pd.DataFrame(list(self.mongo.db[self.collections['robinhood_instruments']].find({},{'_id':1})))
		existing_urls=set(existing_urls['_id'].unique())

		instruments=self.rh.get_all_instruments()
		for row,instrument in instruments.iterrows():
			instrument=instrument.to_dict()
			instrument['instrument']=instrument['url']
			instrument['_id']=instrument['url']
			self.mongo.db[self.collections['robinhood_instruments']].update({"_id":instrument['_id']},instrument,upsert=True)
		processed_urls=set(instruments['url'].unique())
		self.mongo.db[self.collections['robinhood_instruments']].remove({"_id":{"$in":list(existing_urls-processed_urls)}})
		logging.info('finished updating robinhood instruments')
		return
	def update_earnings(self,tickers=None):
	
		self.mongo.create_index(self.collections['robinhood_earnings'],'cik')
		self.mongo.create_index(self.collections['robinhood_earnings'],'ticker')
		self.mongo.create_index(self.collections['robinhood_earnings'],'date')
		
		if tickers is None:
			tickers=self.cq.existing_tickers()
		for ticker in tickers:
			logging.info('updating earnings for:'+ticker)
			
			existing_earnings=pd.DataFrame(list(self.mongo.db[self.collections['robinhood_earnings']].find({'ticker':ticker})))
			if len(existing_earnings)>0 and len(existing_earnings['cik'].unique())>1:
				logging.info('more than 1 cik, removing from earnings')
				self.mongo.db[self.collections['robinhood_earnings']].remove({'ticker':ticker})
			
			cik=self.cq.ticker2cik(ticker)
			data=self.rh.get_earnings_releases(ticker,identifier='symbol')
			if data is None:
				continue
			data=pd.DataFrame(data)
			if len(data)==0:
				continue
			data=data.sort_values(['year','quarter'])
			
			for index,row in data.iterrows():
				data={}
				id='_'.join([ticker,str(cik),str(row['year']),str(row['quarter'])])
				data['_id']=id
				data['ticker']=ticker
				data['cik']=cik
				data['year']=row['year']
				data['quarter']=row['quarter']
				data['eps_estimate']=row['eps']['estimate']
				data['eps_actual']=row['eps']['actual']
				if row['report'] is not None:
					data['date']=row['report']['date']
				self.mongo.db[self.collections['robinhood_earnings']].update({"_id":id},data,upsert=True)	
			logging.info('ticker earnings updated:'+ticker)
if __name__ == "__main__":
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	rh=RobinhoodUpdater(config_file='finance_cfg.cfg')
	x=rh.update_robinhood_instruments()
	rh.update_earnings()
	#rh.update_earnings(tickers=['SPY'])