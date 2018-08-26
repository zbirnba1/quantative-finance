import logging
import inspect #for argument logging https://stackoverflow.com/questions/10724495/getting-all-arguments-and-values-passed-to-a-python-function
import requestswrapper
import pandas as pd
import io
class QuandlWrapper():
	def __init__(self,api_key="",proxies=None,timeout=300,max_retries=20,error_codes=[500,502,503],internal_error_codes=range(400,500)):
		"""
		apikey = the api key for the quandl account
		proxies=if you want to use a proxy for the auth,poxy='socks5://'+user+':'+password+'@'+SOCKS5_PROXY_HOST+':'+str(SOCKS5_PROXY_PORT),proxies=dict(http=proxy,https=proxy)
		timeout=how long you want to wait for a call to finish
		max_retries= how many times to retry a call
		error_codes=what codes are wrong 
		internal_error_codes=error codes for this api that requries you to stop https://docs.quandl.com/v1.0/docs/troubleshooting#section-httpquandl-error-codes
		
		Some General Help:
		https://blog.quandl.com/getting-started-with-the-quandl-api
		"""
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
		self.api_url="https://www.quandl.com/api/v3"
		self.tables_url="/datatables"
		self.timeseries_url="/datasets"        
		return
	def tables_dataframe(self,datatable_code,params=None):
		if params is None:
			params={}
		"""
		Returns the tables request as a dataframe
		"""
		data=self.issue_tables_request(datatable_code,params,format='.csv')
		if data is None:
			return None
		str=data.text
		df=pd.read_csv(io.StringIO(str))
		if len(df)>9999:
			exit()
		return df
	def timeseries_dataframe(self,database_code,dataset_code,params=None):
		if params is None:
			params={}
		"""
		Returns the timeseries request as a dataframe
		"""
		data=self.issue_timeseries_request(database_code, dataset_code, params, return_format='.csv')
		if data is None:
			return None
		str=data.text
		df=pd.read_csv(io.StringIO(str))
		if len(df)>9999:
			exit()
		return df
	def issue_tables_request(self,datatable_code,params=None,format='.json'):
		if params is None:
			params={}
		"""
		find more info here https://docs.quandl.com/v1.0/docs/tables-1
		"""
		params['api_key']=self.api_key
		url=self.api_url+self.tables_url+'/'+datatable_code+format
		response=self.connector.issue_request(url=url,method='GET',params=params)
		return response
	def issue_timeseries_request(self,database_code,dataset_code,params=None,return_format='.json'):
		if params is None:
			params={}
		"""
		Find more info here https://docs.quandl.com/v1.0/docs/time-series
		"""
		params['api_key']=self.api_key
		url=self.api_url+self.timeseries_url+'/'+database_code+'/'+dataset_code+'/data'+return_format
		response=self.connector.issue_request(url=url,method='GET',params=params)
		return response
if __name__ == "__main__":
	pass