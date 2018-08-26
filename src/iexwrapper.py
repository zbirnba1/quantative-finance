import requestswrapper
import inspect
import logging
import json
class IexWrapper():
	def __init__(self,token=None):
		self.token=token
		self.connector=requestswrapper.RequestsWrapper()
		self.api_url='https://api.iextrading.com/1.0'
		return
	def get_data(self,url='',method='get',params=None):
		if params is None:
			params={}
		data=self.connector.issue_request(url=url,method=method,params=params)
		if data is None:
			return None
		data=data.json()
		return data
	def get_ref_data_symbols(self):
		url="https://api.iextrading.com/1.0/ref-data/symbols"
		data=self.connector.issue_request(url=url,method='get')
		if data is None:
			return None
		data=data.json()
		return data
	def get_short_interest(self,symbol):
		endpoint='/stock/'+symbol.lower()+'/short-interest'
		params={'token':self.token}
		data=self.connector.issue_request(url=self.api_url+endpoint,method='get')
		if data is None:
			return None
		data=data.json()
		return data
	def get_earnings(self,symbol):
		endpoint='/stock/'+symbol+'/earnings'
		return self.get_data(url=self.api_url+endpoint)
	def get_financials(self,symbol,period='quarter'):
		endpoint='/stock/'+symbol+'/financials'
		params={'period':period}
		return self.get_data(url=self.api_url+endpoint,method='get',params=params)
	def get_stats(self,symbol):
		endpoint='/stock/'+symbol+'/stats'
		return self.get_data(url=self.api_url+endpoint)
	def get_peers(self,symbol):
		endpoint='/stock/'+symbol+'/peers'
		return self.get_data(url=self.api_url+endpoint)
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
