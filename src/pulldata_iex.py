import iexwrapper
import logging
import inspect
import configwrapper
import mongomanager
import pandas as pd

class IexUpdater():
	def __init__(self,config_file=None):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		self.iexwrapper = iexwrapper.IexWrapper(token=self.config.get_string('IEX', 'token'))
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		return
	def get_iex_companies_df(self):
		collection = self.config.get_string('FINANCIALDATA_COLLECTIONS', 'iex_symbols')
		df=pd.DataFrame(list(self.mongo.db[collection].find()))
		return df
	def update_iex_symbols(self):
		collection=self.config.get_string('FINANCIALDATA_COLLECTIONS','iex_symbols')
		self.mongo.create_collection(collection=collection)
		self.mongo.create_index(collection=collection,index='symbol')
		self.mongo.create_index(collection=collection, index='iexId')

		data=self.iexwrapper.get_ref_data_symbols()
		if data is None or len(data)==0:
			logging.info('iex data is empty, not updating')
			return
		for company in data:
			company['_id']=int(company['iexId'])
			self.mongo.db[collection].update({'_id':company['_id']},company,upsert=True)
		return
	def update_iex_stats(self):
		collection=self.config.get_string('FINANCIALDATA_COLLECTIONS','iex_stats')
		self.mongo.create_collection(collection=collection)
		self.mongo.create_index(collection=collection,index='symbol')
		companies=self.get_iex_companies_df()
		for index,company in companies.iterrows():
			stats=self.iexwrapper.get_stats(symbol=company['symbol'])
			if stats is None or len(stats)==0:
				continue
			stats['_id']=company["_id"]
			self.mongo.db[collection].update({'_id':company['_id']},stats,upsert=True)
if __name__ == "__main__":
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	i=IexUpdater(config_file='finance_cfg.cfg')
	print i.update_iex_symbols()
