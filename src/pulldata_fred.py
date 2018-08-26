import fredwrapper
import mongomanager
import logging
import inspect
import pandas as pd
import configwrapper

class FredUpdater():
	def __init__(self,config_file,proxies=None,timeout=300,max_retries=50,error_codes=[500,503],internal_error_codes=[401,403,404,429]):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		if proxies is None:
			proxies={}
		self.fred=fredwrapper.FredWrapper(api_key=self.config.get_string('FRED','api_key'),proxies=proxies,timeout=timeout,max_retries=max_retries)
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		self.mongo.create_collections(self.collections.values())
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_many_fred_timeseries_data(self,series_ids,full_update=False):
		for id in series_ids:
			self.update_fred_timeseries_data(id,full_update)
	def update_fred_timeseries_data(self,series_id,full_update=False):
		self.mongo.create_index(self.collections['fred_series_observations'],'date')
		self.mongo.create_index(self.collections['fred_series_observations'],'series_id')
		#first we get the existing data for that series
		if full_update is True:
			self.mongo.db[self.collections['fred_series_observations']].remove({'series_id':series_id})
		df=pd.DataFrame(list(self.mongo.db[self.collections['fred_series_observations']].find({'series_id':series_id})))
		observation_start=None
		if len(df)!=0:
			observation_start=pd.to_datetime(df['date']).max().date().strftime('%Y-%m-%d')
			logging.info('observation_start:'+str(observation_start))
		data=self.fred.get_series(series_id=series_id,observation_start=observation_start)
		if data is None or len(data)==0:
			logging.error('data is none or empty')
			return None
		data['series_id']=series_id
		data=data.sort_index()
		for index,row in data.iterrows():
			info=row.to_dict()
			info['date']=index
			id='_'.join([series_id,index])
			info['_id']=id
			self.mongo.db[self.collections['fred_series_observations']].update({'_id':id},info,upsert=True)
		return
if __name__ == "__main__":

	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	f=FredUpdater(config_file='finance_cfg.cfg')

	f.update_many_fred_timeseries_data(list(set(
		['DDDM01USA156NWDB', 'GDP', 'WILL5000PRFC', 'RU3000TR', 'SP500', 'CPROFIT', 'DBAA', 'DAAA', 'T10Y3M', 'DGS3MO',
		 'DGS10', 'DGS1', 'DGS5', 'DGS2', 'USRECD'])))
	f.update_many_fred_timeseries_data(["DSPIC96", "RRSFS", "PAYEMS", "INDPRO"])
	f.update_many_fred_timeseries_data(["DTB1YR", "DTB3"])

	f.update_fred_timeseries_data('DDDM01USA156NWDB')
	exit()
	x=f.update_fred_timeseries_data('DBAA')
	x=f.update_fred_timeseries_data('DAAA')
	x=f.update_fred_timeseries_data('T10Y3M')
	x=f.update_fred_timeseries_data('SP500')
	x=f.update_fred_timeseries_data('CPROFIT')
	f.update_many_fred_timeseries_data(list(set(['DGS3MO','DGS10','DGS1','DGS5','DGS2','USRECD','DDDM01USA156NWDB'])))
	f.update_many_fred_timeseries_data(["DTB1YR", "DTB3", 'DGS10', 'DGS1'])