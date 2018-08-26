#Author Zach
#this object is a wrapper for the python requests package.

import requests
import logging
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session, exceptions
import inspect #for argument logging https://stackoverflow.com/questions/10724495/getting-all-arguments-and-values-passed-to-a-python-function
import collections

class RequestsWrapper():
	
	"""
	The requests wrapper is a general method for issueing requests, automatic retries, error catching.
	"""

	def __init__(self,max_retries=20,timeout=300,proxies=None,auth=None,error_codes=range(500,600),internal_error_codes=range(400,500),requests_list_size=50):
		"""
		max_retries=the number of times to retry a failed request
		timeout=how long to wait for a call before it is considered failed
		proxies=any socks5 proxy you may want to use
		auth=the auth to use for all calls
		error_codes=the error codes to retry on
		requests_list_size = the size of the memory of previous requests
		"""
		frame=inspect.currentframe()
		args, _, _, values = inspect.getargvalues(frame)
		functionname=inspect.getframeinfo(frame).function
		logging.info(functionname+str([(i, values[i]) for i in args]))
		
		if proxies is None:
			proxies={}
		if auth is None:
			auth=()
		
		self.s=requests.Session()
		self.s.mount('https://',HTTPAdapter(max_retries=Retry(total=max_retries,status_forcelist=error_codes))) #http://www.coglib.com/~icordasc/blog/2014/12/retries-in-requests.html
		if proxies is None:
			proxies={}
		if auth is None:
			auth=()
		self.auth=auth
		self.proxies=proxies
		self.timeout=timeout
		self.internal_error_codes=internal_error_codes
		self.requests_list=collections.deque(maxlen=requests_list_size) #https://stackoverflow.com/questions/5944708/python-forcing-a-list-to-a-fixed-size #a list containing the latest requests
		self.responses_list=collections.deque(maxlen=requests_list_size)
		return
	def issue_request(self,url,method='GET',data=None,params=None,headers=None,auth=None,proxies=None,timeout=None):        
		frame=inspect.currentframe()
		args, _, _, values = inspect.getargvalues(frame)
		functionname=inspect.getframeinfo(frame).function
		logging.info(functionname+str([(i, values[i]) for i in args]))
		
		#set the defaults to those found in the object
		if data is None:
			data={}
		if params is None:
			params={}
		if headers is None:
			headers={}
		if auth is None:
			auth=self.auth
		if proxies is None:
			proxies=self.proxies
		if timeout is None:
			timeout=self.timeout

		try:
			response=self.s.request(url=url,method=method,data=data,params=params,headers=headers,auth=auth,proxies=proxies,timeout=timeout) #verify="cas/ngbrac-squid.crt" http://docs.python-requests.org/en/master/_modules/requests/api/
		except requests.exceptions.RequestException as e:
			logging.error(e)
			return None
		  
		request_params={}
		request_params['url']=url
		request_params['method']=method
		request_params['data']=data
		request_params['params']=params
		request_params['headers']=headers
		self.requests_list.append(request_params)
		self.responses_list.append(response)
		
		if response.status_code in self.internal_error_codes: #if it failed for some other eason besides timeout, or a 5xx error, then just return None which means the request failed
			logging.error(response.content)
			return None
		if len(response.content)==0: #there is nothing to return
			logging.error('No content found')
			return None          
		return response
if __name__ == "__main__":
	pass
