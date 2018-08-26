import requestswrapper
import pandas as pd
import logging
import inspect
import io
import time

class AlphaVantage():
	def __init__(self,api_key,proxies=None,timeout=300,max_retries=20,error_codes=range(500,600),internal_error_codes=range(400,500)):
		if proxies is None:
			proxies={}
		self.api_key=api_key
		self.max_retries=max_retries
		self.timeout=timeout
		self.proxies=proxies
		self.error_codes=error_codes
		self.internal_error_codes=internal_error_codes
		
		self.connector=requestswrapper.RequestsWrapper(max_retries=self.max_retries,timeout=self.timeout,proxies=self.proxies,error_codes=self.error_codes,internal_error_codes=self.internal_error_codes)

		self.api_url="https://www.alphavantage.co/query"
	
	def issue_request(self,params=None):
		time.sleep(2) #so we dont run into any API timing issues or errosr
		if params is None:
			params={}
		params['apikey']=self.api_key
		try:
			response=self.connector.issue_request(url=self.api_url,method='GET',params=params)
		except Exception as e:
			logging.error(e.message)
			logging.error(e.args)
			return None
		if response is None:
			return None
		if response.status_code!=200:
			logging.error('bad status code')
			logging.error(response.status_code)
			logging.error(response.content)
			exit()
		if 'Error' in response.content:
			logging.error('bad request')
			logging.error(response.content)
			return None	
		return response
	def time_series_adjusted_daily(self,symbol,outputsize='full',datatype='csv'):
		params={}
		params['function']='TIME_SERIES_DAILY_ADJUSTED'
		params['symbol']=symbol
		params['outputsize']=outputsize.lower() #just in case some passes it in as capitals
		params['datatype']=datatype.lower()
		response=self.issue_request(params)
		return response
	#had to add the helper function because of the way that we redid the columns
	def get_pandas_time_series_adjusted_daily_helper(self,symbol,outputsize):
		response=self.time_series_adjusted_daily(symbol,outputsize,datatype='JSON')
		if response is None:
			return None
		data=response.json()
		if 'Time Series (Daily)' not in data:
			return None
		data=data['Time Series (Daily)']
		if len(data)==0:
			return None
		df=pd.DataFrame()
		for date in data:
			newdata=data[date]
			newdata['timestamp']=date
			df=df.append(newdata,ignore_index=True)
		old_cols=df.columns
		new_cols=[col.split('.')[-1].strip().replace(' ','_') for col in old_cols]
		df.columns=new_cols
		
		if outputsize.lower()=='full':
			compact_df=self.get_pandas_time_series_adjusted_daily_helper(symbol,outputsize='compact')
			if compact_df is not None and len(compact_df)>0:
				df=df.append(compact_df,ignore_index=True)
		return df
	def get_pandas_time_series_adjusted_daily(self,symbol,outputsize='full'):
		df=self.get_pandas_time_series_adjusted_daily_helper(symbol,outputsize)
		if df is None or len(df)==0:
			return None
		df=df.drop_duplicates('timestamp')
		df['timestamp']=pd.to_datetime(df['timestamp'])
		df=df.set_index('timestamp')
		df=df.sort_index()
		for col in df:
			df[col]=df[col].astype('float')
		return df

if __name__ == "__main__":
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	pass