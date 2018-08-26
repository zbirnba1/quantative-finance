import logging
import inspect #for argument logging https://stackoverflow.com/questions/10724495/getting-all-arguments-and-values-passed-to-a-python-function
import requestswrapper
import pandas as pd
import io
from sets import Set
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import copy
import threading
import uuid
import json
import numpy as np

class RobinHoodWrapper():
	def __init__(self,username=None,password=None,token=None,proxies=None,timeout=300,max_retries=100,error_codes=[500,503],internal_error_codes=range(400,500),instruments=None,options_instruments=None):
		if proxies is None: #Mystery Solved: http://docs.python-guide.org/en/latest/writing/gotchas/ ( Mutable Default Arguments)
			proxies={}
		self.api_url = "https://api.robinhood.com"
		self.username=username
		self.password=password
		self.connector=requestswrapper.RequestsWrapper(max_retries=max_retries,timeout=timeout,proxies=proxies,error_codes=error_codes,internal_error_codes=internal_error_codes)
		self.instruments=instruments
		self.options_instruments=options_instruments

		self.token=None

		self.oauth2_token=None
		self.oauth2_refresh_token=None
		self.oauth2_token_expires_time=datetime.now()

		if username is not None and password is not None and token is None:
			self.token=self.login(username,password) #the item that holds the auth that we can pass once we login
		if username is None and password is None and token is not None:
			self.token=token

		tspp=pd.read_csv("http://tsp.finra.org/finra_org/ticksizepilot/TSPilotSecurities.txt",sep="|") #https://support.robinhood.com/hc/en-us/articles/214848443--0-05-Increment-Orders
		self.tspp=list(tspp['Ticker_Symbol'])
		return

	def login(self,username=None,password=None):
		if username is None:
			username=self.username
		if password is None:
			password=self.password
		url="https://api.robinhood.com/api-token-auth/"
		data={'username':username,'password':password}
		data=self.issue_request(url=url,method="post",data=data)
		token='Token '+data['token']
		self.token=token
		return token
	def oauth_login(self):
		data = {'expires_in': 86400,'client_id': 'c82SH0WZOsabOXGP2sxqcj34FxkvfnWRZBKlBjFS','grant_type': 'password','scope': 'internal','username': self.username, 'password': self.password}
		result=self.issue_request(url=self.api_url+'/oauth2/token/',method='post',data=data)
		self.oauth2_token=result['token_type']+' '+ result['access_token']
		self.oauth2_refresh_token=result['refresh_token']
		self.oauth2_token_expires_time=datetime.now()+relativedelta(seconds=result['expires_in'])
		return
	def migrate_token(self):
		#START PATCH 08/20/2018
		self.oauth_login()
		return
		#END PATCH 08/20/2018

		endpoint='/oauth2/migrate_token/'
		result=self.issue_request(url=self.api_url+endpoint,method="post",auth=True)
		self.oauth2_token=result['token_type']+' '+ result['access_token']
		self.oauth2_refresh_token=result['refresh_token']
		self.oauth2_token_expires_time=datetime.now()+relativedelta(seconds=result['expires_in'])
		return
	def refresh_token(self):
		return
	def revoke_token(self):
		url="https://api.robinhood.com/oauth2/revoke_token/"
		result=self.issue_request(url=url,method="post",auth=True)
		return
	def logout(self):
		url="https://api.robinhood.com/api-token-logout/"
		self.issue_request(url,"post",auth=True)
		self.logged_in = False
		self.token=None
		self.oauth2_token=None
		self.oauth2_refresh_token=None
		self.oauth2_token_expires_time=datetime.now()
		return
	def get_accounts(self):
		url="https://api.robinhood.com/accounts/"
		data=self.multi_page_request(url=url,method="get",auth=True)
		return data
		
	def get_portfolio(self,account=None):
		if account is None:
			account=self.get_accounts()[0]
		url=account['portfolio']
		data=self.issue_request(url,"get",auth=True)
		return data
		
	def get_orders(self):
		url="https://api.robinhood.com/orders/"
		orders=self.multi_page_request(url,"get",auth=True)
		orders_df=pd.DataFrame()
		for item in orders:
			orders_df=orders_df.append(item,ignore_index=True)
		return orders_df
	def cancel_all_orders(self,orders=None):
		if orders is None:
			orders=self.get_orders()
		for index,row in orders.iterrows():
			if row['cancel']!=None:
				id=row['id']
				self.cancel_order(id)
		logging.info('all orders canceled')		
		return
	def cancel_order(self,orderid):
		url="https://api.robinhood.com/orders/"+orderid+"/cancel/"
		cancelorder=self.issue_request(url,"post",auth=True)
		logging.info('order canceled:'+str(cancelorder))
		return cancelorder		
	def get_positions(self,account=None):
		if account is None:
			account=self.get_accounts()[0]
		url=account['positions']
		data=self.multi_page_request(url,"get",auth=True)
		positions_df=pd.DataFrame()
		for item in data:
			positions_df=positions_df.append(item,ignore_index=True)
		if len(positions_df)==0:
			return positions_df
		positions_df=positions_df[positions_df['quantity'].astype('float')>0]
		positions_df['symbol']=positions_df['instrument'].apply(self.instrument2symbol_fast)
		positions_df['last_trade_price']=positions_df['instrument'].apply(self.get_last_price_fast)
		positions_df['shares_held']=0
		for column in ['shares_held_for_buys','shares_held_for_options_collateral','shares_held_for_options_events','shares_held_for_sells','shares_held_for_stock_grants','shares_pending_from_options_events']:
			positions_df['shares_held']=positions_df['shares_held']+positions_df[column].astype('float')
		return positions_df		
	def get_position_details(self,position):
		data=self.issue_request(position,auth=True)
		return data
	def multi_page_request(self,url,method='GET',data=None,params=None,headers=None,auth=False,oauth2=False):
		if data is None:
			data={}
		if params is None:
			params={}
		if headers is None:
			headers={}
		results=[]
		requestresp=self.issue_request(url=url,method=method,data=data,params=params,headers=headers,auth=auth,oauth2=oauth2)
		if requestresp==None:
			return None
		
		requestsdata=requestresp
		results+=requestsdata['results']
		while requestsdata['next']!=None:
			requestresp=self.issue_request(url=requestsdata['next'],method=method,data={},params={},headers=headers,auth=auth,oauth2=oauth2) #clear out the params and data because we keps on passing these things over and over and over again, causing the URL to get to big (7/7/18)
			if requestresp==None:
				return None
			requestsdata=requestresp
			results+=requestsdata['results']
		return results
	def single_page_request(self,url,method='GET',data=None,params=None,headers=None,auth=False,oauth2=False):
		if data is None:
			data={}
		if params is None:
			params={}
		if headers is None:
			headers={}
		requestresp=self.issue_request(url=url,method=method,data=data,params=params,headers=headers,auth=auth,oauth2=oauth2)
		if requestresp==None:
			return None
		data=requestresp['results']
		return data
	def issue_request(self,url,method='GET',data=None,params=None,headers=None,auth=False,oauth2=False):
		if data is None:
			data={}
		if params is None:
			params={}
		if headers is None:
			headers={}

		if auth is True:
			headers['Authorization'] = self.token
		if oauth2 is True:
			if datetime.now()>self.oauth2_token_expires_time:
				self.migrate_token()
			headers['Authorization'] = self.oauth2_token

		response=self.connector.issue_request(url=url,method=method,params=params,data=data,headers=headers)        
		if response is None:
			packetresp=self.connector.responses_list[-1]
			if packetresp.status_code in [400,404]:
				logging.error(packetresp.status_code)
				logging.error(packetresp.content)
				return None
			elif packetresp.status_code in [200]:
				return response
			else:
				logging.error('unknown error, we need to address this')
				logging.error(packetresp.status_code)
				logging.error(packetresp.content)
				exit()
		return response.json()
	def get_instrument_by_symbol(self,symbol):
		params={}
		params['symbol']=symbol
		params['active_instruments_only']=True
		instrument=self.multi_page_request(url=self.api_url+'/instruments/',params=params)
		if instrument is None or len(instrument)==0:
			return None
		instrument=instrument[0]
		return instrument
	#START OPTIONS CODE
	def get_options_suitability(self):
		endpoint='/options/suitability'
		url=self.api_url+endpoint
		results=self.issue_request(url=url,auth=True)
		return results
	def can_trade_options(self):
		options_suitability=self.get_options_suitability()
		if options_suitability is None:
			return False
		elif 'max_option_level' not in options_suitability:
			return False
		elif options_suitability['max_option_level']=='option_level_3':
			return True
		else:
			return False
	def get_all_options_instruments(self):
		if self.options_instruments is not None:
			logging.info('already have options_instruments')
			return self.options_instruments
		endpoint='/options/instruments/'
		results=self.multi_page_request(url=self.api_url+endpoint,oauth2=True)
		if results is None:
			return None
		self.options_instruments=pd.DataFrame(results)
		return self.options_instruments
	def get_option_chains(self,symbol):
		instrument=self.get_instrument_by_symbol(symbol)
		if instrument is None:
			return None
		instrumentid=instrument['id']
		params={}
		params['equity_instrument_ids']=instrumentid
		url=self.api_url+'/options/chains/'
		chains=self.multi_page_request(url,params=params,auth=True)
		chain_ids=[]
		for chain in chains:
			chain_ids.append(chain['id'])
		return chain_ids
	def get_options_instruments(self,symbol):
		chains=self.get_option_chains(symbol)
		if chains is None or len(chains)==0:
			return None
		df=pd.DataFrame()
		for chain in chains:
			params={}
			params['chain_id']=chain
			endpoint='/options/instruments/'
			results=self.multi_page_request(url=self.api_url+endpoint,params=params,auth=True)
			df=df.append(pd.DataFrame(results),ignore_index=True)
		return df
	def get_options_instrument_data(self,symbol):
		df=self.get_options_instruments(symbol=symbol)
		if df is None or len(df)==0:
			return None
		df=df[df['tradability']=='tradable']
		df = df[df['state'] == 'active']
		if df is None or len(df)==0:
			return None
		for index,option in df.iterrows():
			params={}
			params['instruments']=option['url']
			extra_data=self.single_page_request(url=self.api_url+'/marketdata/options/',params=params,oauth2=True)
			if extra_data is None:
				continue
			for item in extra_data[0]:
				df.loc[index,item]=extra_data[0][item]
		return df
	def get_options_instrument_quote_details(self,options_instrument_url):
		params={}
		params['instruments'] = options_instrument_url
		details = self.single_page_request(url=self.api_url + '/marketdata/options/', params=params, oauth2=True)
		if details is None:
			return None
		return details[0]
	def get_options_instruments_details(self,options_instrument_url):
		details=self.issue_request(url=options_instrument_url)
		return details

	def get_options_orders(self):
		url=self.api_url+'/options/orders/'
		result=self.multi_page_request(url=url,method="get",auth=True)
		if result is None or len(result)==0:
			return None
		return pd.DataFrame(result)
	def cancel_all_options_orders(self):
		options_df=self.get_options_orders()
		if options_df is None or len(options_df)==0:
			return
		options_df=options_df[pd.notnull(options_df['cancel_url'])]
		for row,option in options_df.iterrows():
			self.cancel_option_order(option['cancel_url'])
		return
	def cancel_option_order(self,cancel_url):
		result=self.issue_request(url=cancel_url,method="post",auth=True)
		return result
	def get_options_positions(self):
		url=self.api_url+'/options/positions/'
		result=self.multi_page_request(url=url,method='get',auth=True)
		if result is None or len(result)==0:
			return pd.DataFrame()
		result=pd.DataFrame(result)
		result=result[result['quantity'].astype('float')>0]
		for index,option in result.iterrows():
			options_instrument=self.get_options_instruments_details(options_instrument_url=option['option'])
			for key in ['type']:
				result.loc[index,key]=options_instrument[key]
		return result
	def get_options_position(self,position_id):
		#we should really not need to call this because we can get them all at once with the get_options_positions code
		url=self.api_url+'/options/positions/'+position_id
		result=self.issue_request(url=url,auth=True)
		return result
	def get_options_events(self):
		url=self.api_url+'/options/events/'
		result = self.multi_page_request(url=url, auth=True)
		return result
	def get_options_events_df(self):
		options_events=self.get_options_events()
		df=pd.DataFrame()
		#print options_events
		for event in options_events:
			for component in event['equity_components']:
				info=component.copy()
				info['event_date']=event['event_date']
				df=df.append(info,ignore_index=True)
		return df
	#END OPTIONS CODE
	def get_all_instruments(self):
		if self.instruments is not None:
			logging.info('already have instruments')
			return self.instruments
		endpoint="/instruments/"
		url=self.api_url+endpoint
		results=self.multi_page_request(url=url)
		if results is None:
			return None
		self.instruments=pd.DataFrame(results)
		return self.instruments
	def get_earnings_releases(self,value,identifier='symbol'):
		endpoint='/marketdata/earnings/'
		params={}
		params[identifier]=value
		url=self.api_url+endpoint
		data=self.single_page_request(url,params=params)
		return data
	def filter_instruments_df(self,key,value):
		if self.instruments is None:
			self.get_all_instruments()
		s=self.instruments[key]==value
		df=self.instruments[s]
		# s=df['tradeable']==1
		# df=df[s]
		s=df['state']=='active'
		df=df[s]
		if len(df)>1:
			logging.info('more than one quote in df')
			logging.info('key:'+str(key))
			logging.info('value:'+str(value))
			logging.info(df)
			exit()
		if len(df)==1:
			return df.ix[df.index[0]].to_dict() #return as a dict
		if len(df)==0:
			logging.info(str(value)+'is not valid, 0 matches')
			return None
	def instrument2symbol(self,instrument):
		x=self.filter_instruments_df(key='url',value=instrument)
		if x is None:
			return None
		else:
			return x['symbol']
	def instrument2symbol_fast(self,instrument):
		instrument_data=self.get_instrument_data(instrument)
		if pd.isnull(instrument_data):
			return None
		return str(instrument_data['symbol'])

	def symbol2instrument(self,symbol):
		x=self.filter_instruments_df(key='symbol',value=symbol)
		if x is None:
			return None
		else:
			return x['url']
	def symbol2instrumentid(self,symbol):
		x=self.filter_instruments_df(key='symbol',value=symbol)
		if x is None:
			return None
		else:
			return x['id']
	def get_quote(self,symbol):
		d=self.filter_instruments_df(key='symbol',value=symbol)
		if d==None:
			return None
		quoteurl=d['quote']
		quote=self.issue_request(quoteurl)
		if quote is None:
			return None
		else:			
			return quote
	def get_fundamentals(self,symbol):
		d=self.filter_instruments_df(key='symbol',value=symbol)
		if d==None:
			return None
		fundurl=d['fundamentals']
		fundamentals=self.issue_request(fundurl)
		if fundamentals is None:
			return None
		else:			
			return fundamentals			
	def get_last_price(self,symbol):
		quote=self.get_quote(symbol)
		if quote is None:
			return None
		else:
			return float(quote['last_trade_price'])
	def get_last_price_fast(self,instrument_url):
		instrument_data=self.get_instrument_data(instrument_url)
		if pd.isnull(instrument_data):
			return None
		quote = self.issue_request(instrument_data['quote'])
		if quote is None:
			return None
		else:
			return float(quote['last_trade_price'])

	def get_splits_by_symbol(self,symbol):
		instrument_data=self.get_instrument_by_symbol(symbol)
		if pd.isnull(instrument_data):
			return None
		return self.get_splits_by_instrument_data(instrument_data=instrument_data)
	def get_splits_by_instrument_data(self,instrument_data):
		if instrument_data is None:
			return None
		if 'splits' not in instrument_data or pd.isnull(instrument_data['splits']):
			return None
		result=self.multi_page_request(url=instrument_data['splits'])
		if len(result)==0:
			return None
		result=pd.DataFrame(result)
		result=result.sort_values('execution_date')
		return result
	def get_instrument_data(self,instrument_url):
		result=self.issue_request(url=instrument_url)
		return result

	def round_to_value(self,number,roundto): #will round things to the nearest .05
		return (round(number / roundto) * roundto)

	def submitt_option_order(self,position_effect='open',options_instrument_url_id=None,order_type='limit',quantity=0,price=None,account=None,time_in_force='gfd'):
		if options_instrument_url_id is None:
			logging.error('no options instrument id specified')
			return None
		if position_effect is None:
			return None
		if account is None:
			account=self.get_accounts()[0]['url']
		quote=self.get_options_instrument_quote_details(options_instrument_url_id)
		if quote is None:
			return None
		if price is None:
			price=float(quote['bid_price'])
		quantity=int(quantity)
		if quantity>0:
			side='buy'
			direction='debit'
		elif quantity<0:
			side="sell"
			direction='credit'
			quantity=quantity*-1
		else:
			logging.error('cant determine side')
			return None
		data={}
		data['account']=account
		data['direction']=direction
		data['legs']=[{'side':side,'option':options_instrument_url_id,'position_effect':position_effect,'ratio_quantity':1}]
		data['price']=float(price)
		data['quantity']=float(quantity)
		data['ref_id']=str(uuid.uuid4())
		data['time_in_force']=time_in_force
		data['type']=order_type
		headers={'Accept-Encoding': 'gzip, deflate','Accept': '*/*'}
		headers['Content-type']='application/json; charset=utf-8'
		result=self.issue_request(url=self.api_url+'/options/orders/',method="post",data=json.dumps(data),headers=headers,auth=True)
		return result
	def submitt_order(self,symbol,quantity,trigger='immediate',price=None,time_in_force='gfd',account=None):
		#there is no such thing as a market order, if you dont supply a price we assume a limit with 5% gain and loss on the current price.
		quote=self.get_quote(symbol)
		if quote is None:
			logging.info(symbol+':order not submitted, quote is None')
			return None
		if account is None:
			account=self.get_accounts()[0]
		logging.info(quote)
		
		if quantity>0:
			side="buy"
		if quantity<0:
			side="sell"
			quantity=-1*quantity #convert it to a positive number
		if quantity==0:
			logging.info('buying 0 shares of:'+symbol)
			return None
			
		if price==None: #if there is no price, assume the last traded price
			type='market'
			if side=='buy':
				price=float(quote['last_trade_price'])*(1.05) #if it is a limit order add 5% to the price
			if side=='sell':
				price=float(quote['last_trade_price'])*(.95) #if it is a sell, subtract 5% from the price
			type='limit' #we now change it to a limit order

		else:
			type='limit'
		if symbol in self.tspp:
			price=self.round_to_value(price,.05) #https://support.robinhood.com/hc/en-us/articles/214848443--0-05-Increment-Orders
		price=float(price)
		price=round(price,2)
		
		#test to make sure we can afford the purchase
		buyingpower=float(account['buying_power'])
		if side=="buy" and price*quantity>buyingpower:
			logging.info('buying_power:'+str(buyingpower))
			logging.info('quantity:'+str(quantity))
			logging.info('symbol:'+symbol)
			logging.info('price:'+str(price))
			logging.info('we dont have enough money to purchase this')
			return None
		
		data={}
		data['account']=account['url']
		data['instrument']=quote['instrument']
		data['symbol']=symbol
		data['type']=type
		data['time_in_force']=time_in_force
		data['trigger']=trigger
		data['quantity']=quantity
		data['side']=side
		
		#THIS IS NOT USED< FIX THIS (4/5/17)
		#now the logic
		if type=='limit' or type=='market':
			data['price']=price
		if trigger=='stop':
			data['stop_price']=price
			
		logging.info(symbol+':order data')
		logging.info(data)
		
		url="https://api.robinhood.com/orders/"
		orderdata=self.issue_request(url,"post",data=data,auth=True)
		return orderdata
	def get_ach_relationships(self):
		url="https://api.robinhood.com/ach/relationships/"
		ach_relationships=self.multi_page_request(url,"get",auth=True)
		ach_relationships_df=pd.DataFrame()
		for item in ach_relationships:
			ach_relationships_df=ach_relationships_df.append(item,ignore_index=True)
		return ach_relationships_df
	def get_ach_transfers(self):
		url="https://api.robinhood.com/ach/transfers/"
		ach_transfers=pd.DataFrame(self.multi_page_request(url,"get",auth=True))
		return ach_transfers
	def bank2rh(self,amount,id,direction='deposit',frequency=''):
		relationships=self.get_ach_relationships()
		relationships=relationships[relationships['verified']==1]
		relationships=relationships[relationships['id']==id]
		if len(relationships)==0:
			logging.error('no relationships found')
			return None
		url=relationships['url'].iloc[0]
		data={}
		data['ach_relationship']=str(url)
		data['amount']=float(amount)
		data['frequency']=frequency
		data['direction']=direction
		url="https://api.robinhood.com/ach/transfers/"
		result=self.issue_request(url,"post",data=data,auth=True)
		logging.info(result)
		return	
	def get_filled_orders(self):
		orders=self.get_orders()
		orders=orders[orders['state']=='filled']
		return orders
	def get_non_filled_orders(self):
		orders = self.get_orders()
		orders=orders[orders['state'].isin(['queued','unconfirmed','confirmed','partially_filled'])]
		return orders
	def get_adjusted_orders_df(self):
		orders=self.get_filled_orders()

		if len(orders)==0:
			return orders

		splits_df=pd.DataFrame()
		for instrument in list(orders['instrument'].unique()):
			instrument_data=self.get_instrument_data(instrument)
			split_data=self.get_splits_by_instrument_data(instrument_data)
			splits_df=splits_df.append(split_data,ignore_index=True)

		#we need to insert any exercised options
		executed_options=self.get_options_events_df()

		inst_sym_price={} #a map containing the symbol and current price of that instrument
		for instrument in list(orders['instrument'].unique()):
			inst_sym_price[instrument]={}
			inst_sym_price[instrument]['symbol']=self.instrument2symbol_fast(instrument)
			inst_sym_price[instrument]['last_trade_price']=self.get_last_price_fast(instrument)
		if len(executed_options)>0: #go through the options
			for instrument in list(executed_options['instrument'].unique()):
				if instrument not in inst_sym_price:
					inst_sym_price[instrument] = {}
					inst_sym_price[instrument]['symbol'] = self.instrument2symbol_fast(instrument)
					inst_sym_price[instrument]['last_trade_price'] = self.get_last_price_fast(instrument)

		adjusted_orders_df=pd.DataFrame()
		for index,order in orders.iterrows():
			for execution in order['executions']:
				ordertime=pd.to_datetime(execution['timestamp'])
				info={}
				info['ordertime']=ordertime
				info['quantity']=float(execution['quantity'])
				info['instrument']=order['instrument']
				info['side']=order['side']
				info['symbol']=inst_sym_price[order['instrument']]['symbol']
				info['last_trade_price']=inst_sym_price[order['instrument']]['last_trade_price']
				info['orderid']=order['id']
				info['executionid']=execution['id']
				info['price']=float(execution['price']) #the order price
				adjusted_orders_df=adjusted_orders_df.append(info,ignore_index=True)

		for index,option in executed_options.iterrows():
			info={}
			info['price']=float(option['price'])
			info['instrument']=option['instrument']
			info['ordertime']=pd.to_datetime(option['event_date'])
			info['quantity']=float(option['quantity'])
			info['side']=option['side']
			info['last_trade_price'] = inst_sym_price[option['instrument']]['last_trade_price']
			info['symbol'] = inst_sym_price[option['instrument']]['symbol']
			adjusted_orders_df = adjusted_orders_df.append(info, ignore_index=True)

		#now we need to adjust the order df for splits
		for index,split in splits_df.iterrows():
			split_instrument=split['instrument']
			matching_orders=adjusted_orders_df[adjusted_orders_df['instrument']==str(split_instrument)]
			matching_orders=matching_orders[pd.to_datetime(matching_orders['ordertime'])<=pd.to_datetime(split['execution_date'])] #orders which happened before the split time
			if len(matching_orders)==0:
				continue
			adjusted_orders_df.loc[matching_orders.index,'price']=adjusted_orders_df.loc[matching_orders.index,'price']*float(split['divisor'])/float(split['multiplier'])
			adjusted_orders_df.loc[matching_orders.index,'quantity']=adjusted_orders_df.loc[matching_orders.index,'quantity']*float(split['multiplier'])/float(split['divisor'])
		#we assume that if we bought less then 1 share, then we didn't buy anything...this is a problem that we wont solve now (6/27/18)
		adjusted_orders_df['quantity']=np.floor(adjusted_orders_df['quantity'])
		adjusted_orders_df=adjusted_orders_df[adjusted_orders_df['quantity']>=1]
		adjusted_orders_df=adjusted_orders_df.sort_values('ordertime')
		return adjusted_orders_df
	def get_by_share_adjusted_orders_df(self):
		orders=self.get_adjusted_orders_df()
		orders2=pd.DataFrame()
		for index,row in orders.iterrows():
			for i in range(int(row['quantity'])):
				data=copy.deepcopy(row.to_dict())
				data['quantity']=1
				orders2=orders2.append(data,ignore_index=True)
		orders2=orders2.sort_values('ordertime')
		return orders2
	def get_wash_sale_symbols(self):
		orders=self.get_by_share_adjusted_orders_df()
		washdf=pd.DataFrame()
		for instrument in list(orders['instrument'].unique()):
			buys=orders[(orders['instrument']==instrument) & (orders['side']=='buy')]
			sells=orders[(orders['instrument']==instrument) & (orders['side']=='sell')]
			buys=buys.sort_values('ordertime')
			sells=sells.sort_values('ordertime')
			i=0
			for index,trade in sells.iterrows():
				buyprice=float(buys.iloc[i]['price'])
				sellprice=float(trade['price'])
				if sellprice<buyprice: #only if we lost money
					washdf=washdf.append(trade.to_dict(),ignore_index=True)
				i+=1
		if len(washdf)==0:
			return [] #return an empty list
		else:
			washdf=washdf[pd.to_datetime(washdf['ordertime']).dt.date>datetime.utcnow().date()-relativedelta(days=35)]
			washsymbols=list(washdf['symbol'].unique())
			logging.info('washsymbols:'+str(washsymbols))
			return washsymbols

	def get_portfolio_history(self,span='year',interval='day',bounds='regular',account=None):
		if account is None:
			account=self.get_accounts()[0]
		accountnumber=account['account_number']
		params={}
		params['span']=span
		params['bounds']=bounds
		params['interval']=interval

		url="https://api.robinhood.com/portfolios/historicals/"+accountnumber
		data = self.issue_request(url, 'get', params=params,auth=True)
		df=pd.DataFrame()
		for item in data['equity_historicals']:
			df=df.append(item,ignore_index=True)
		return df
	#will return a dataframe of each share that is CURRENTLY in the portfolio, assuming FIFO model
	#Will return error if we own something that we dont have, or if the sums dont match up
	def get_current_portfolio_by_orders(self):
		df=pd.DataFrame() #initilize dataframe
		non_filled_orders=self.get_non_filled_orders()
		if len(non_filled_orders)>0:
			non_filled_orders=non_filled_orders[non_filled_orders['side']=='sell'] #if there are any outstanding sells orders for any stock
		if len(non_filled_orders)>0:
			logging.error('we have outstanding non-filled orders which will effect how we construct the current portfolio from orders')
			exit()
		orders=self.get_by_share_adjusted_orders_df()
		positions=self.get_positions()
		position_instruments=set(positions['instrument'].unique()) #set of instruments we currently have
		order_instruments=set(orders['instrument'].unique())
		neverpurchased=list(position_instruments-order_instruments) #a list of things that we own, but never purchased
		neverpurchased=positions[positions['instrument'].isin(neverpurchased)]
		if len(neverpurchased)>0:
			logging.error(neverpurchased)
			logging.error('there is something on file that we never purchased')
			neverpurchased.to_csv('never_purchased.csv')
			exit()
		for instrument in list(position_instruments):
			buys=orders[(orders['instrument']==instrument) & (orders['side']=='buy')]
			if len(buys)==0:
				logging.error('there is something on file that we never purchased')
				exit()
				continue
			existing_shares=positions[positions['instrument']==instrument]['quantity'].astype('float').iloc[0]
			if existing_shares>len(buys): #we just take all the buys that we do have
				logging.error('we did not buy enough for what we currenlty have:'+str(existing_shares))
				buys=buys
			else: #we take the last N buy orders which makes us equal to the number of shares we should have...
				buys = buys.iloc[-int(existing_shares):]
			buys=buys.sort_values('ordertime')
			df=df.append(buys,ignore_index=True)
		df=df.sort_values('ordertime')
		df['percent_loss']=df['last_trade_price']/df['price']-1
		return df
	
	#figure out what we need to sell today, keeps track of tax losses
	#returns a series with the index being the ticker symbol and the value being the number of shares to sell today
	def get_sell_items(self):
		selldf = self.get_current_portfolio_by_orders()
		selldf = selldf.sort_values('ordertime')
		positions=self.get_positions()
		shares_held_positions=positions[positions['shares_held']>0]
		for index,row in shares_held_positions.iterrows():
			instrument=row['instrument']
			shares_held=int(np.ceil(row['shares_held']))
			company_orders=selldf[selldf['instrument']==instrument]
			company_orders=company_orders.sort_values('ordertime')
			drop_company_orders=company_orders.iloc[-shares_held:] #remove the last N shares we have ordered to be held
			selldf=selldf.drop(index=drop_company_orders.index)
		for index,row in selldf.iterrows():
			if row['last_trade_price']>row['price']: #it is a gain, price is the price you purchased it for
				selldf.loc[index,'selldate']=pd.to_datetime(row['ordertime'])+relativedelta(years=1)+relativedelta(days=10)
			else: #it is a loss
				selldf.loc[index,'selldate']=pd.to_datetime(row['ordertime'])+relativedelta(years=1)-relativedelta(days=10)
		selldf['selldate']=selldf['selldate'].dt.date
		selldf['ordertime']=selldf['ordertime'].dt.date
		selldf=selldf.sort_values('selldate')
		selldf=selldf[['symbol','ordertime','selldate','price','last_trade_price']]
		selldf=selldf[pd.to_datetime(selldf['selldate']).dt.date<=datetime.now().date()]
		selldf2=pd.Series() #contains the series that we will will actually return
		for ticker in selldf['symbol'].unique():
			selldf2.loc[ticker]=-1*len(selldf[selldf['symbol']==ticker])
		return selldf2
	#pass in the list of ticker symbols we want to sell last, and the ammount we want to keep
	#RETURNS A SERIES
	def get_ammount_selldf(self,recommended_df_symbols_list,sellammount):
		cansellshares=self.get_current_portfolio_by_orders()
		cansellshares.loc[cansellshares['symbol'].isin(recommended_df_symbols_list),'in_recommended_df']=True
		cansellshares.loc[~(cansellshares['symbol'].isin(recommended_df_symbols_list)),'in_recommended_df']=False
		cansellshares['last_trade_priceadj']=cansellshares['last_trade_price']*.9 #the minimum price we expect to get if we sell this stock
		
		cansellshares=cansellshares.sort_values(['in_recommended_df','percent_loss'])
		cansellshares['cumsum']=cansellshares['last_trade_priceadj'].cumsum()
		temp=cansellshares[~(cansellshares['cumsum'].astype('float')>=float(sellammount))]
		tosell=cansellshares.iloc[:len(temp)+1,:]
		selldf=pd.Series()
		for ticker in tosell['symbol'].unique():
			selldf.loc[ticker]=len(tosell[tosell['symbol']==ticker])		
		return selldf
	def get_total_portfolio_value(self):
		current_portfolio = self.get_current_portfolio_by_orders()
		return current_portfolio['last_trade_price'].sum()

if __name__ == "__main__":
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	pass