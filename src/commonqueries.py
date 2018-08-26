import logging
import inspect
import mongomanager
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import pytz
from datetime import datetime
from dateutil.relativedelta import relativedelta

class CommonQueries():
	def __init__(self,host='127.0.0.1',port=27017,dbname='mydb',username=None,password=None,collections=None):
		if collections is None:
			collections={}
		self.mongo=mongomanager.MongoManager(host=host,port=port,username=username,password=password,dbname=dbname)
		self.collections=collections
		self.companies=None
		self.mongo.create_collections(self.collections.values())
		self.clean_dbs()
	def clean_dbs(self):
		return
	def existing_ciks(self):
		if self.companies is None:
			self.companies=self.get_companies()
		allciks=self.companies
		ciklist=list(allciks['cik'].unique())
		return ciklist
	def existing_tickers(self):
		if self.companies is None:
			self.companies=self.get_companies()
		alltickers=self.companies
		tickerlist=list(alltickers['ticker'].unique())
		return tickerlist
	def get_company_name(self,cik):
		company=self.get_company(cik)
		if company is None or len(company)==0:
			return None
		company=(company.iloc[0]).to_dict()
		if 'name' not in company:
			return None
		else:
			return str(company['name'])
	def get_company(self,cik):
		if self.companies is None:
			self.companies=self.get_companies()	
		return self.companies[self.companies['cik']==cik]
	def get_companies(self):
		allinfo=list(self.mongo.db[self.collections['intrinio_companies']].find({}))
		allinfo=pd.DataFrame(allinfo)
		allinfo=allinfo[pd.notnull(allinfo['latest_filing_date'])]
		allinfo=allinfo[pd.notnull(allinfo['ticker'])]
		allinfo=allinfo[pd.notnull(allinfo['cik'])]
		allinfo=allinfo.sort_values('latest_filing_date')
		allinfo=allinfo[allinfo['standardized_active']==True]
		allinfo=allinfo.drop_duplicates('ticker',keep='last')
		return allinfo		
	def ticker2cik(self,ticker):
		if pd.isnull(ticker):
			return None
		company=self.mongo.db[self.collections['intrinio_companies']].find_one({'ticker':ticker,'cik':{'$exists':True}})
		if company is None or company['cik'] is None:
			return None
		else:
			return company['cik']
	def ticker2figi(self,ticker,micorder=None):
		cik=self.ticker2cik(ticker)
		if cik is None:
			return None
		figi=self.cik2figi(cik,micorder)
		return figi
	def cik2figi(self,cik,micorder=None):
		if micorder is None:
			micorder=['XNYS','XNAS']
		if cik is None:
			return None
		company=self.mongo.db[self.collections['intrinio_companies']].find_one({'cik':cik,'securities':{'$exists':True}})
		if company is None:
			return None
		securities=pd.DataFrame(company['securities'])
		if securities is None or len(securities)==0:
			return None
		elif 'mic' not in securities.columns:
			logging.error('mic not in columns for company:'+company['cik'])
			return None
		elif micorder is None and 'primary_listing' in securities.columns and len(securities[securities['primary_listing']==True])>0:
			return securities[securities['primary_listing']==True]['figi'].iloc[0]
		elif micorder is not None:
			for mic in micorder:
				if len(securities[securities['mic']==mic])>0:
					return securities[securities['mic']==mic]['figi'].iloc[0]
		else:
			logging.error('converting cik to figi:'+cik)
			securities.to_csv('badsecurities.csv')
			logging.error('we should never be here')
			exit()
	def get_statements(self,cik):
		available_statements=set([x['_id'] for x in self.mongo.db[self.collections['intrinio_standardized_fundamentals']].find({'cik':cik},{"_id":1})])
		downloaded_statements=set([x['_id'] for x in self.mongo.db[self.collections['intrinio_standardized_financials']].find({"_id":{"$in":list(available_statements)}},{"_id":1})])
	
		statements=pd.DataFrame(list(self.mongo.db[self.collections['intrinio_standardized_fundamentals']].find({"_id":{"$in":list(downloaded_statements)}})))
		if len(statements)==0:
			return statements
		statements=statements[pd.notnull(statements['fiscal_year'])]
		statements=statements[pd.notnull(statements['fiscal_period'])]
		statements=statements[pd.notnull(statements['statement_type'])]
		statements=statements.sort_values('filing_date')
		statements=statements.drop_duplicates(['fiscal_year','fiscal_period','statement_type'],keep='last')
		return statements
	def get_last_market_cap2(self,cik):
		company=self.get_company(cik)
		if company is None or len(company)==0 or len(company)>1:
			return 0
		company=company.iloc[0]
		ticker=company['ticker']
		if ticker is None:
			return 0
		iex_stats=self.mongo.db[self.collections['iex_stats']].find_one({'symbol':ticker})
		if iex_stats is None or 'marketcap' not in iex_stats:
			return 0
		market_cap=float(iex_stats['marketcap'])
		return market_cap
	def get_last_market_cap(self,cik):
		statements=self.get_statements(cik)
		if len(statements)==0:
			return 0
		statements=statements[statements['statement_type']=='calculations']
		if len(statements)==0:
			return 0
		statements=statements[statements['fiscal_period'].isin(['Q1','Q2','Q3','Q4'])]
		statements=statements.sort_values(['end_date'])
		if len(statements)==0:
			return 0
		id=statements.iloc[-1]['_id']
		result=self.mongo.db[self.collections['intrinio_standardized_financials']].find_one({"_id":id})
		if result is None or 'marketcap' not in result or result['marketcap']=='nm':
			return 0
		else:
			return float(result['marketcap'])
	def get_quarter_df(self,cik):
		df=pd.DataFrame()
		statements=self.get_statements(cik)
		if statements is None or len(statements)==0:
			return None
		statements=statements[statements['fiscal_period'].isin(['Q1','Q2','Q3','Q4'])]
		if len(statements)==0:
			logging.info('empty quarter statement for cik:'+cik)
			return None	
		for index,data in statements.iterrows():
			result=self.mongo.db[self.collections['intrinio_standardized_financials']].find_one({"_id":data['_id']})
			if result is None:
				continue
			result.update(data)
			yearquarter=float(data['fiscal_year'])*4+float(data['fiscal_period'].replace('Q',''))
			#df.set_value(yearquarter,'yearquarter',yearquarter)
			df.loc[yearquarter,'yearquarter']=yearquarter
			for item in result:
				d=str(result[item])
				if d=='nm':
					d=np.NaN
				#df.set_value(yearquarter,item,d)
				df.loc[yearquarter,item]=d
		if len(df)==0:
			return None
		#filter out any years that are more than 2 away from this year
		df=df[df['fiscal_year'].astype('int')<=int((datetime.now().year)+2)]
		df=df.sort_index()
		quarterly_index=list(df.index)
		true_quarterly_index=range(int(quarterly_index[0]),int(quarterly_index[-1])+1,1)
		#insert the blank rows for skips
		for yearquarter in true_quarterly_index:
			if yearquarter not in quarterly_index:
				#df.set_value(yearquarter,'yearquarter',yearquarter)
				df.loc[yearquarter,'yearquarter']=yearquarter
		df=df.sort_index()
		return df
	def get_annual_df(self,cik):
		df=pd.DataFrame()
		statements=self.get_statements(cik)
		if statements is None or len(statements)==0:
			return None
		statements=statements[statements['fiscal_period']=='FY']
		if len(statements)==0:
			logging.info('empty annual statement for cik:'+cik)
			return None
		for index,data in statements.iterrows():
			result=self.mongo.db[self.collections['intrinio_standardized_financials']].find_one({"_id":data['_id']})
			if result is None:
				continue
			result.update(data)
			year=data['fiscal_year']
			yearquarter=float(year)*4+float(4)
			#df.set_value(yearquarter,'yearquarter',yearquarter)
			df.loc[yearquarter,'yearquarter']=yearquarter
			for item in result:
				d=str(result[item])
				if d=='nm':
					d=np.NaN
				#df.set_value(yearquarter,item,d)
				df.loc[yearquarter,item]=d
		if len(df)==0:
			return None
		#filter out any years that are more than 2 away from this year
		df=df[df['fiscal_year'].astype('int')<=int((datetime.now().year)+2)]
		#now we insert the skips
		df=df.sort_index()
		annual_index=list(df.index)
		true_annual_index=range(int(annual_index[0]),int(annual_index[-1])+4,4)
		for yearquarter in true_annual_index:
			if yearquarter not in annual_index:
				#df.set_value(yearquarter,'yearquarter',yearquarter)
				df.loc[yearquarter,'yearquarter']=yearquarter
		df=df.sort_index()
		return df
	def get_fixed_quarter_df(self,cik):
		qdf=self.get_quarter_df(cik)
		adf=self.get_annual_df(cik)

		if qdf is None or adf is None or len(qdf)==0 or len(adf)==0:
			return None
			
		all_tags=[str(x['tag']) for x in list(self.mongo.db[self.collections['intrinio_standardized_tags_and_labels']].find({}))]
		balance_sheet_tags=[str(x['tag']) for x in list(self.mongo.db[self.collections['intrinio_standardized_tags_and_labels']].find({'statement_type':'balance_sheet'}))]
		for tag in all_tags:
			if tag in qdf.columns:
				qdf[tag]=qdf[tag].astype('float')
			if tag in adf.columns:
				adf[tag]=adf[tag].astype('float')

		for tag in balance_sheet_tags:
			if tag not in qdf.columns or tag not in adf.columns:
				continue
			s=qdf[tag].isnull()
			tempfd=qdf[s][tag]
			for index,oldvalue in tempfd.iteritems():
				yearquarter=index
				s=adf.index==yearquarter
				if s.sum()==0:
					continue
				tempfd2=adf[s][tag]
				if len(tempfd2)>1:
					logging.info('an extra item here')
					exit()
				elif len(tempfd2)==0:
					continue
				else:
					#qdf.set_value(yearquarter,tag,tempfd2.iloc[0])
					qdf.loc[yearquarter,tag]=tempfd2.iloc[0]
		return qdf
	def get_alphavantage_prices(self,ticker):
		prices=self.mongo.db[self.collections['alphavantage_prices']].find({'ticker':ticker})
		prices=pd.DataFrame(list(prices))
		if len(prices)==0:
			return None
		if len(prices)!=len(prices['date'].unique()) or len(prices['cik'].unique())>1:
			logging.info('something wrong with prices, removing')
			self.mongo.db[self.collections['alphavantage_prices']].remove({'ticker':ticker})
			return None
		prices=prices.sort_values('date')
		return prices
	def get_intrinio_prices(self,value,key='figi'):
		if key=='cik':
			value=self.cik2figi(value)
		elif key=='ticker':
			value=self.ticker2figi(value)
		elif key=='figi':
			value=value
		else:
			exit()
		key='figi'
		prices=self.mongo.db[self.collections['intrinio_prices']].find({key:value})
		prices=pd.DataFrame(list(prices))
		if len(prices)==0:
			logging.info('empty prices for key:'+key)
			logging.info('empty prices for value:'+value)
			return None	
		prices=prices.sort_values('date')
		if len(prices)!=len(prices['date'].unique()) or len(prices['figi'].unique())>1:
			logging.error('something wrong with prices, removing, key:'+key+' value:'+value)
			self.mongo.db[self.collections['intrinio_prices']].remove({key:value})
			return None	
		return prices
	def get_intrinio_index_prices(self,ticker):
		prices=self.mongo.db[self.collections['intrinio_prices']].find({'ticker':ticker})
		prices=pd.DataFrame(list(prices))
		if len(prices)==0:
			return None
		if len(prices)!=len(prices['date'].unique()):
			logging.info('something wrong with prices, removing')
			self.mongo.db[self.collections['intrinio_prices']].remove({'ticker':ticker})
			return None
		for column in ['close','open','high','low','volume']:
			if column in prices.columns:
				prices['adj_'+column]=prices[column]
		prices=prices.sort_values('date')
		return prices
	def get_prices(self,ticker): #Change the function depending on where we want to get the pricesfrom	
		return self.get_alphavantage_prices(ticker)
		
	def get_all_tags(self):
		all_tags=[str(x['tag']) for x in list(self.mongo.db[self.collections['intrinio_standardized_tags_and_labels']].find({}))]
		return all_tags
	def get_next_release(self,cik):
		data=self.mongo.db[self.collections['metrics']].find_one({'cik':cik})
		if data is None:
			return None
		else:
			return data['next_release']
	def get_next_release2(self,cik):
		date=self.mongo.db[self.collections['robinhood_earnings']].find({'cik':cik,"date":{"$gt":datetime.now().strftime('%Y-%m-%d')}},{"date":1,"_id":0}).sort("date",1).limit(1)
		if date is None:
			return None
		else:
			return date[0]['date']
	def get_days_until_date(self,date):
		date=pd.to_datetime(date)
		now=pd.to_datetime(datetime.now())
		return int((date-now).days)
	def get_last_complete_market_day(self):
		#will return a string of the last open market day (that has closed, so the prices are no longer updating)
		x=mcal.get_calendar('NYSE').schedule(start_date=datetime.now().date()-relativedelta(days=7),end_date=datetime.now().date()+relativedelta(days=7))
		now = pytz.utc.localize(datetime.utcnow())
		x=x[x['market_close']<=now]
		return pd.to_datetime(x.index[-1]).strftime('%Y-%m-%d')
	def get_market_days(self,period=relativedelta(years=1)):
		#will return a list of previous in perioddays that the market has been open up to the last valid day
		x=mcal.get_calendar('NYSE').schedule(start_date=datetime.now().date()-period,end_date=datetime.strptime(self.get_last_complete_market_day(),'%Y-%m-%d'))
		return x.index.strftime('%Y-%m-%d') #return everything as strings
	def get_robinhood_instruments(self):
		instruments=pd.DataFrame(list(self.mongo.db[self.collections['robinhood_instruments']].find({})))
		return instruments
	def get_insider_transactions(self,cik):
		if cik is None:
			return None
		form_4s=list(self.mongo.db[self.collections['sec_form4_xmls']].find({'company_cik':cik,'non_derivative_transactions_list':{'$ne':None}},{'non_derivative_transactions_list':1}))
		transactions=pd.DataFrame()
		for form in form_4s:
			t=form['non_derivative_transactions_list']
			id=form['_id']
			ownerinfo=self.get_owner_info(id)
			for trade in t:
				trade['accno']=form['_id']
				trade.update(ownerinfo)
				transactions=transactions.append(trade,ignore_index=True)
		for column in ['director','officer','other_relation','ten_percent_owner']:
			if column in transactions.columns:
				transactions[column]=transactions[column].astype('bool')
		return transactions
	def get_total_purchase_value_for_insiders(self,cik,period=relativedelta(years=1)):
		if cik is None:
			return None
		transactions=self.get_insider_transactions(cik)
		if transactions is None or len(transactions)==0:
			return None
		transactions=transactions[transactions['transaction_type_code']=='P']
		transactions=transactions[pd.to_datetime(transactions['transaction_date']).dt.date>=datetime.now().date()-period]
		if len(transactions)==0:
			return None
		transactions['transaction_price']=transactions['transaction_price'].astype('float')
		transactions['ammount_of_shares']=transactions['ammount_of_shares'].astype('float')
		transactions['value']=transactions['transaction_price']*transactions['ammount_of_shares']
		return transactions['value'].sum()
	def get_owner_info(self,accno):
		if pd.isnull(accno):
			return {}
		form=self.mongo.db[self.collections['sec_form4_xmls']].find_one({'_id':accno,'owner_info_list':{'$exists':True}},{'owner_info_list':1})
		if form is None:
			return {}
		owners=form['owner_info_list']
		if len(owners)==0:
			return {}
		owner=owners[0]
		return owner
	def last_form4_date(self,cik):
		last_one=self.mongo.db[self.collections['intrinio_filings']].find_one({'cik':cik,'report_type':'4'},sort=[("filing_date",-1)])
		if last_one is None:
			return None
		else:
			return last_one['filing_date']
	def get_percent_greater_than(self,collection,cik=None,key=None):
		#the collection is the collection we want to search
		#the cik is the cik of the reference company
		#they key item that we want to look up for that company
		#designed for use with the metrics db
		value=self.mongo.db[collection].find_one({'cik':cik,key:{'$exists':True}},{key:1})
		if value is None:
			return None
		value=value[key]
		num_greater=self.mongo.db[collection].find({key:{'$gte':value}}).count()
		total_num=self.mongo.db[collection].find({key:{'$exists':True}}).count()
		if total_num is None or num_greater is None or total_num==0 or num_greater==0:
			return None
		percent=float(num_greater)/float(total_num)
		return percent
	def get_value_from_metrics(self,collection,cik,key):
		value=self.mongo.db[collection].find_one({'cik':cik,key:{'$exists':True}},{key:1})
		if value is None:
			return None
		value=value[key]
		return value
	def convert_option_chain_rh2td(self,symbol,stock_price=None,option_chain=pd.DataFrame()):
		"""
		converts the robinhood option chain data into a tdameritrade option chain, so we can use the get_best_put to sell and the get_best_call_to sell functions natively without modification
		"""
		if option_chain is None or len(option_chain)==0:
			return None
		for index,option in option_chain.iterrows():
			option_chain.loc[index,'days-to-expiration']=(pd.to_datetime(option['expiration_date']).date()-datetime.now().date()).days
			option_chain.loc[index,'bid']=option['bid_price']
			option_chain.loc[index,'ask']=option['ask_price']
			option_chain.loc[index,'strike']=option['strike_price']
			option_chain.loc[index,'c/p']=option['type']
			option_chain.loc[index,'price']=stock_price
			option_chain.loc[index,'option-symbol']=option['instrument']
		option_chain['ticker']=symbol
		option_chain['symbol']=symbol
		return option_chain
	def get_best_put_to_sell(self,symbol,option_chain=pd.DataFrame(),collection='metrics',collections=None,exercise_fee=20.00,trading_fee=6.95,contract_fee=.75,max_strike_price=None):
		if collections is None:
			collections=self.collections
		if option_chain is None or len(option_chain)==0:
			return None
		emyield=self.get_value_from_metrics(collections[collection],self.ticker2cik(symbol),'emyield')
		if emyield is None:
			return None
		options_df=option_chain
		options_df['days-to-expiration'] = options_df['days-to-expiration'].astype('int')
		options_df['bid'] = options_df['bid'].astype('float')
		options_df['strike'] = options_df['strike'].astype('float')
		options_df['price'] = options_df['price'].astype('float')
		options_df = options_df[pd.notnull(options_df['bid'])]
		options_df = options_df[options_df['c/p'] == 'put']
		options_df = options_df[options_df['strike'] < options_df['price']]
		options_df['return']=((((options_df['bid']*100-(1*trading_fee)-contract_fee-exercise_fee)/(100*options_df['strike']))+1)**(365/(options_df['days-to-expiration'].astype('float'))))-1
		options_df = options_df[options_df['return'] >= emyield]
		if max_strike_price is not None: #so we dont take up too much of the portfolio with a single purchase
			options_df=options_df[options_df['strike']<=float(max_strike_price)]

		if len(options_df) == 0:
			return None
		next_release = self.get_next_release(self.ticker2cik(symbol))
		if next_release is None:
			days_to_release = 0
		else:
			days_to_release = int((pd.to_datetime(next_release).date() - datetime.now().date()).days)

		options_df['days_to_release'] = days_to_release
		options_df = options_df[options_df['days_to_release'] > options_df[
			'days-to-expiration']]  # only get ones where the days to release is greater than the days to expiration
		if len(options_df) == 0:
			return None

		prices_df = self.get_intrinio_prices(symbol, 'ticker')
		if prices_df is None or len(prices_df) == 0:
			return None
		if pd.to_datetime(self.get_last_complete_market_day()) != pd.to_datetime(prices_df['date']).max():
			return None

		for deltaday in list(options_df['days-to-expiration'].unique()):
			change=self.get_pct_change(prices_df,deltaday)
			daytempoptions=options_df[options_df['days-to-expiration']==deltaday]
			for index,option in daytempoptions.iterrows():
				rec_decline=(option['strike']/option['price'])-1
				options_df.loc[index,'mos']=float((change>rec_decline).sum())/float(len(change))
		options_df = options_df[pd.notnull(options_df['mos'])]
		options_df = options_df[pd.notnull(options_df['return'])]
		options_df = options_df[options_df['days-to-expiration'] > 0]
		options_df = options_df.sort_values('mos', ascending=False)
		options_df = options_df.drop_duplicates(subset=['ticker'], keep='first')
		if len(options_df) == 0:
			return None
		return options_df.iloc[0]['option-symbol']
	def get_best_call_to_sell(self,symbol,option_chain=pd.DataFrame(),collection='metrics',collections=None,exercise_fee=20.00,trading_fee=6.95,contract_fee=.75,max_strike_price=None):
		if collections is None:
			collections=self.collections
		if option_chain is None or len(option_chain)==0:
			return None
		emyield=self.get_value_from_metrics(collections[collection],self.ticker2cik(symbol),'emyield')
		if emyield is None:
			return None
		options_df=option_chain
		options_df['days-to-expiration']=options_df['days-to-expiration'].astype('int')
		options_df['bid']=options_df['bid'].astype('float')
		options_df['ask']=options_df['ask'].astype('float')
		options_df['strike']=options_df['strike'].astype('float')
		options_df['price']=options_df['price'].astype('float')
		options_df=options_df[pd.notnull(options_df['bid'])]
		options_df=options_df[options_df['c/p']=='call']
		options_df=options_df[options_df['strike']>options_df['price']]
		if max_strike_price is not None: #so we dont take up too much of the portfolio with a single purchase
			options_df=options_df[options_df['strike']<=float(max_strike_price)]
		options_df['return']=((((options_df['bid']*100-(1*trading_fee)-contract_fee-exercise_fee)/(100*options_df['price']))+1)**(365/(options_df['days-to-expiration'].astype('float'))))-1
		options_df=options_df[options_df['return']>=emyield]
		
		if len(options_df)==0:
			return None
		next_release=self.get_next_release(self.ticker2cik(symbol))
		if next_release is None:
			days_to_release=0
		else:
			days_to_release=int((pd.to_datetime(next_release).date()-datetime.now().date()).days)
		
		options_df['days_to_release']=days_to_release
		options_df=options_df[options_df['days_to_release']>options_df['days-to-expiration']] #only get ones where the days to release is greater than the days to expiration
		if len(options_df)==0:
			return None
		prices_df=self.get_intrinio_prices(symbol,'ticker')
		if prices_df is None or len(prices_df)==0:
			return None
		if pd.to_datetime(self.get_last_complete_market_day())!=pd.to_datetime(prices_df['date']).max():
			return None
			
		for deltaday in list(options_df['days-to-expiration'].unique()):
			change=self.get_pct_change(prices_df,deltaday)
			daytempoptions=options_df[options_df['days-to-expiration']==deltaday]
			for index,option in daytempoptions.iterrows():
				rec_increase=(option['strike']/option['price'])-1
				options_df.loc[index,'mos']=float((change<rec_increase).sum())/float(len(change))
		options_df=options_df[pd.notnull(options_df['mos'])]
		options_df=options_df[pd.notnull(options_df['return'])]
		options_df=options_df[options_df['days-to-expiration']>0]
		options_df=options_df.sort_values('mos',ascending=False)
		options_df=options_df.drop_duplicates(subset=['ticker'],keep='first')
		if len(options_df)==0:
			return None
		return options_df.iloc[0]['option-symbol']
		#the option_chain format is what we expect from tdameritrade, we may want to later swap this out for robinhood once that becomes available
	def get_option_return(self,bid,price,days_to_expiration,trading_fee=6.95,contract_fee=.75,exercise_fee=20.00):
		result = ((((bid*100-trading_fee-contract_fee-exercise_fee)/(100*price))+1)** (365 / (days_to_expiration))) - 1
		return result
	def get_put_option_mos(self,symbol,strike_price,current_price,days_to_expiration):
		prices_df = self.get_intrinio_prices(symbol, 'ticker')
		if prices_df is None or len(prices_df) == 0:
			return None
		if pd.to_datetime(self.get_last_complete_market_day()) != pd.to_datetime(prices_df['date']).max():
			return None
		change = self.get_pct_change(prices_df, int(days_to_expiration))
		rec_decline = (strike_price / current_price) - 1
		mos = float((change > rec_decline).sum()) / float(len(change))
		return mos
	def get_call_option_mos(self,symbol,strike_price,current_price,days_to_expiration):
		prices_df = self.get_intrinio_prices(symbol, 'ticker')
		if prices_df is None or len(prices_df) == 0:
			return None
		if pd.to_datetime(self.get_last_complete_market_day()) != pd.to_datetime(prices_df['date']).max():
			return None
		change = self.get_pct_change(prices_df, int(days_to_expiration))
		rec_increase = (strike_price / current_price) - 1
		mos = float((change < rec_increase).sum()) / float(len(change))
		return mos

	def get_pct_change(self,prices_df,deltaday):
		prices_df['pct_change']=prices_df['adj_close'].pct_change(periods=deltaday)
		return prices_df['pct_change']
	def get_fred_df(self,series_id=None):
		if series_id is None:
			return None
		df=pd.DataFrame(list(self.mongo.db[self.collections['fred_series_observations']].find({'series_id':series_id})))
		df=df.sort_values('date')
		return df
	def recession_indication(self):
		#will return true if we are in a recession, or a downtrend and should active some protection
		spyprices = self.get_intrinio_prices(value='BBG000BDTBL9',
		                                   key='figi')  # because sometimes the index does not update in time...

		if pd.to_datetime(spyprices['date'].max())!=pd.to_datetime(self.get_last_complete_market_day()):
			logging.error('our SPY prices are not up to date')
			exit()

		industrialproduction = self.get_fred_df('INDPRO')
		realretailsales=self.get_fred_df('RRSFS')
		nonfarmemployment=self.get_fred_df('PAYEMS')
		realpersonalincome=self.get_fred_df('DSPIC96')

		industrialproduction = (industrialproduction['value'].iloc[-1] / industrialproduction['value'].max()) - 1
		nonfarmemployment = (nonfarmemployment['value'].iloc[-1] / nonfarmemployment['value'].max()) - 1
		realretailsales = (realretailsales['value'].iloc[-1] / realretailsales['value'].max()) - 1
		realpersonalincome = (realpersonalincome['value'].iloc[-1] / realpersonalincome['value'].max()) - 1
		RI = np.mean(
			[industrialproduction, nonfarmemployment, realretailsales, realpersonalincome])  # the recission indicator
		p0 = spyprices[pd.to_datetime(spyprices['date']).dt.date >= (datetime.now().date() - relativedelta(years=1))][
			'adj_close'].iloc[0]
		p1 = spyprices['adj_close'].iloc[-1]
		sp_return = float(p1) / float(p0) - 1
		movingaverage = spyprices['adj_close'].rolling(200).mean().iloc[-1]
		current = spyprices['adj_close'].iloc[-1]

		fed_10yr = float(self.get_fred_df('DGS10').iloc[-1]['value'])/100
		fed_1yr = float(self.get_fred_df('DGS1').iloc[-1]['value'])/100

		if (float(fed_10yr) - float(fed_1yr) <= 0) or sp_return <= fed_1yr or current <= movingaverage or RI <= -.0093:
			return True
		else:
			return False
	def get_last_td_purchase_date(self,symbol,td_history_df):
		if len(td_history_df)==0:
			return None
		#make sure that the history_df is only for a single account
		history = td_history_df[td_history_df['symbol'] == symbol]
		history = history[~(history['buySellCode'] == 'S')]
		history = history[history['quantity'] > 0]
		x=history[(history['symbol']==symbol) & ((history['type']=='TR')|((history['type']=='RD') & (history['subType']=='EX'))|((history['type']=='RD') & (history['subType']=='TP'))|((history['type']=='RD') & (history['subType']=='NC')))]
		x.loc[:, 'executedDate'] = pd.to_datetime(x['executedDate'])
		if len(x)==0:
			return None
		last_date = x['executedDate'].max()
		if pd.isnull(last_date):
			return None
		else:
			return pd.to_datetime(last_date)
	def is_valid_trade(self,cik):
		valid_trade = self.mongo.db[self.collections['metrics']].find_one({'cik':cik})
		if valid_trade is None:
			valid_trade = True #if we dont have any data then get rid of the company...
		else:
			valid_trade = valid_trade['valid_trade']
		return valid_trade
	def has_split(self,cik):
		has_split = self.mongo.db[self.collections['metrics']].find_one({'cik':cik})
		if has_split is None:
			has_split = True #if we dont have any data then get rid of the company...
		else:
			has_split = has_split['split_since_last_statement']
		return has_split
	def get_put_insurance_symbol(self,maximum_drawdown=0,percentage_to_secure=100):
		return
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	pass