import mongomanager
import quandlwrapper
import logging
import inspect
import pandas as pd
import configwrapper

class QuandlUpdater():
	def __init__(self,config_file,proxies=None,timeout=300,max_retries=50,error_codes=[500,503],internal_error_codes=[401,403,404,429],host='localhost',port=27017,username=None,password=None,dbname='finance',collections=None):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		if proxies is None:
			proxies={}
		if collections is None:
			collections={}
		self.collections=collections
		self.quandl=quandlwrapper.QuandlWrapper(api_key=self.config.get_string('QUANDL','api_key'),proxies=proxies,timeout=timeout,max_retries=max_retries)
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_quandl_timeseries_data(self,database_code,dataset_code):
		self.mongo.create_index(self.collections['quandl_timeseries'],'date')
		self.mongo.create_index(self.collections['quandl_timeseries'],'database_code')
		self.mongo.create_index(self.collections['quandl_timeseries'],'dataset_code')
		
		data=self.quandl.timeseries_dataframe(database_code,dataset_code)
		if data is None:
			logging.error('data is none')
			return
		data['database_code']=database_code
		data['dataset_code']=dataset_code
		data.columns=[x.lower() for x in data.columns]
		data['date']=pd.to_datetime(data['date']).dt.strftime("%Y-%m-%d")
		data=data.sort_values('date')
		for index,row in data.iterrows():
			info=row.to_dict()
			id='_'.join([database_code,dataset_code,row['date']])
			info['_id']=id
			self.mongo.db[self.collections['quandl_timeseries']].update({'_id':id},info,upsert=True)
if __name__ == "__main__":	
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	pass