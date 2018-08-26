import logging
import inspect #for argument logging https://stackoverflow.com/questions/10724495/getting-all-arguments-and-values-passed-to-a-python-function
import requestswrapper
import pandas as pd
from datetime import datetime

class FredWrapper():
	def __init__(self,api_key="",proxies=None,timeout=300,max_retries=20,error_codes=[500,502,503],internal_error_codes=range(400,500)):
		frame=inspect.currentframe()
		args, _, _, values = inspect.getargvalues(frame)
		functionname=inspect.getframeinfo(frame).function
		logging.info(functionname+str([(i, values[i]) for i in args]))
		if proxies is None:
			proxies={}
		self.max_retries=max_retries
		self.timeout=timeout
		self.proxies=proxies
		self.error_codes=error_codes
		self.internal_error_codes=internal_error_codes
		self.api_key=api_key
		
		self.connector=requestswrapper.RequestsWrapper(max_retries=self.max_retries,timeout=self.timeout,proxies=self.proxies,auth=(),error_codes=self.error_codes,internal_error_codes=self.internal_error_codes)
	#https://research.stlouisfed.org/docs/api/fred/realtime_period.html
	#https://research.stlouisfed.org/docs/api/fred/series.html
	def get_series(self,series_id,file_type='json',params=None,observation_start=None,observation_end=None,realtime_start=None,realtime_end=None):
		url="https://api.stlouisfed.org/fred/series/observations"
		if params is None:
			params={}
		params['api_key']=self.api_key
		params['file_type']=file_type
		params['series_id']=series_id
		params['observation_start']=observation_start
		params['observation_end']=observation_end
		params['realtime_start']=realtime_start
		params['realtime_end']=realtime_end
		
		response=self.connector.issue_request(url=url,method='GET',params=params)
		if response is None:
			logging.error('get data failed')
			return None
		data=response.json()
		if 'observations' not in data:
			logging.error('no observations found')
			return None
		data=data['observations']
		if len(data)>=100000:
			logging.error('too much data request, cut it up')#this can later be automatically done should it become an issue
			exit()
		df=pd.DataFrame(data)
		df['date']=pd.to_datetime(df['date']).dt.date.astype('str')
		df=df.drop_duplicates('date')
		df=df.set_index('date')
		df=df.sort_index()
		df=df[['value']]
		s=df['value']=='.'
		df=df[~s]
		for col in df:
			df[col]=df[col].astype('float')
		return df

if __name__=="__main__":
	pass