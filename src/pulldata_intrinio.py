import mongomanager
import intriniowrapper
import logging
import inspect
import copy
import pandas as pd
import commonqueries
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import configwrapper

class IntrinioUpdater():
	def __init__(self,config_file,proxies=None,timeout=300,max_retries=50,error_codes=[500,503],internal_error_codes=[401,403,404,429]):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		auth=(self.config.get_string('INTRINIO','username'),self.config.get_string('INTRINIO','password'))
		if auth is None:
			auth=()
		if proxies is None:
			proxies={}
		if collections is None:
			collections={}
		self.mongo = mongomanager.MongoManager(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'))
		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)
		self.intrinio=intriniowrapper.IntrinioWrapper(auth,proxies,timeout,max_retries,error_codes,internal_error_codes)
		self.last_trade_day=pd.to_datetime(self.cq.get_last_complete_market_day())
		return
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def update_company(self,value):
		company_details=self.intrinio.get_companies(identifier=value)
		if company_details is None or 'cik' not in company_details or company_details['cik'] is None or 'latest_filing_date' not in company_details or company_details['latest_filing_date'] is None:
			logging.error('company details is none')
			return False
		self.mongo.db[self.collections['intrinio_companies']].update({'cik':company_details['cik']},company_details,upsert=True)
		logging.info('company update:'+value)
		return True
	def update_companies(self,full_update=False):
		#will update the intrinio_companies collection also includs details in the same collection
		self.mongo.create_index(self.collections['intrinio_companies'],'latest_filing_date')
		self.mongo.create_index(self.collections['intrinio_companies'],'cik',unique=True)
		self.mongo.create_index(self.collections['intrinio_companies'],'ticker')
		
		max_date=self.mongo.db[self.collections['intrinio_companies']].find_one(sort=[("latest_filing_date",-1)])
		logging.info('max_company:'+str(max_date))
		
		if max_date==None or full_update==True:
			max_date=None
		else:
			max_date=max_date['latest_filing_date']
			
		companies=self.intrinio.get_companies(latest_filing_date=max_date)
		
		if companies is None or len(companies)==0:
			logging.info('companies len is 0 or companies is none')
			return False
			
		companies=pd.DataFrame(companies)
		companies=companies[pd.notnull(companies['cik'])]
		companies=companies[pd.notnull(companies['latest_filing_date'])]
		companies=companies[pd.notnull(companies['ticker'])]
		companies=companies.sort_values('latest_filing_date') #so we always process the oldest one first, this way if we ever need to do it again, we have the correct date
		
		for index,company in companies.iterrows():	
			self.update_company(company['cik'])			
		logging.info(str(len(companies))+':companies updated sucessfully')		
		return True
	def update_all_company_filings(self):
		logging.info('Now in func:update_all_company_filings')
		
		self.mongo.create_index(self.collections['intrinio_filings'],'filing_date')
		self.mongo.create_index(self.collections['intrinio_filings'],'cik')
		self.mongo.create_index(self.collections['intrinio_filings'],'accno',unique=True)
		self.mongo.create_index(self.collections['intrinio_filings'],'report_type')
		self.mongo.create_index(self.collections['intrinio_filings'],'period_ended')
		
		self.mongo.create_index(self.collections['intrinio_pull_times'],'collection')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'date')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'cik')
		
		last_trade_day=self.last_trade_day
		logging.info('last_trade_day:'+str(last_trade_day))
		
		max_filing_date=self.mongo.db[self.collections['intrinio_filings']].find_one({},sort=[("filing_date",-1)])
		logging.info('max_filing_date:'+str(max_filing_date))
		
		if max_filing_date is not None and pd.to_datetime(max_filing_date['filing_date'])>=last_trade_day:
			logging.info('we are already up to date on filings, no need to get things, this can wait until tomorrow')
			pass		
			
		if max_filing_date is None or pd.to_datetime(max_filing_date['filing_date'])<=last_trade_day-relativedelta(days=15):
			logging.error('waited too long (15 days) to update filings, need to download all filings for each company, this will take a lot of calls')
			companies=self.cq.get_companies()
			ciks=companies['cik'].unique()
			self.update_filings(ciks=[ciks])
			
		filings=self.intrinio.get_filings() #gets all filings in the past 30 days from intinio
		if filings is None or len(filings)==0:
			logging.error('no filings found when getting the last 30 days of filings for all companies')
			return
		filings=pd.DataFrame(filings)
		filings=filings.sort_values('accepted_date')
		
		min_filings_date=pd.to_datetime(filings['filing_date']).min()
		logging.info('min_filings_date:'+str(min_filings_date))
		
		ciks=list(filings['cik'].unique())
		for cik in ciks:
			logging.info('updating filing for cik:'+cik)
			cik_filings=filings[filings['cik']==cik].copy(deep=True)
			last_filing_pull=self.mongo.db[self.collections['intrinio_pull_times']].find_one({"cik":cik,'collection':self.collections['intrinio_filings']})
			last_already_pulled_cik_filing=self.mongo.db[self.collections['intrinio_filings']].find_one({"cik":cik},sort=[("filing_date",-1)])
			
			if last_filing_pull is None and last_already_pulled_cik_filing is not None:
				last_filing_pull=pd.to_datetime(last_already_pulled_cik_filing['filing_date']).date()
			elif last_filing_pull is not None:
				last_filing_pull=pd.to_datetime(last_filing_pull['date']).date()
			else:
				last_filing_pull=None
				
			logging.info('last_filing_pull:'+str(last_filing_pull))
			if last_filing_pull is None:
				logging.info('doing a full update of filings for cik:'+cik)
				self.update_filings(ciks=[cik],full_update=True)	
			elif last_filing_pull is not None and last_filing_pull < min_filings_date.date():
				logging.info('doing a parital of filings for cik:'+cik)
				self.update_filings(ciks=[cik])			
			elif last_filing_pull is not None and last_filing_pull >= min_filings_date.date():
				logging.info('only updating what we need to, the last pull we have is greater than the min in the last 30 days')
				cik_filings['cik']=cik
				cik_filings['_id']=cik_filings['accno']
				cik_filings=cik_filings.sort_values('accepted_date')
				for index,filing in cik_filings.iterrows():
					filing_data=filing.to_dict()
					self.mongo.db[self.collections['intrinio_filings']].update({'accno':filing_data['accno']},filing_data,upsert=True)
				logging.info(str(len(cik_filings))+': filings updated for:'+cik)
				logging.info('update filings pull date')
			else:
				logging.error(str(last_filing_pull))
				logging.error((pd.to_datetime(last_filing_pull['date'])).date())
				logging.error(min_filings_date.date())
				logging.error('we should never get here')
				exit()
			pull_data={'cik':cik,'collection':self.collections['intrinio_filings'],'date':last_trade_day.strftime('%Y-%m-%d')}
			self.mongo.db[self.collections['intrinio_pull_times']].update({"cik":cik,'collection':self.collections['intrinio_filings']},pull_data,upsert=True)
		return
		
	def update_filings(self,full_update=False,ciks=None):
	
		last_trade_day=self.last_trade_day
		
		self.mongo.create_index(self.collections['intrinio_filings'],'filing_date')
		self.mongo.create_index(self.collections['intrinio_filings'],'cik')
		self.mongo.create_index(self.collections['intrinio_filings'],'accno',unique=True)
		self.mongo.create_index(self.collections['intrinio_filings'],'report_type')
		self.mongo.create_index(self.collections['intrinio_filings'],'period_ended')
		
		self.mongo.create_index(self.collections['intrinio_pull_times'],'collection')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'date')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'cik')
		
		if ciks is None: #pass in a list of ciks that we want to update
			companies=self.cq.get_companies()
			ciks=companies['cik'].unique()
			
		for cik in ciks:
			company=self.cq.get_company(cik)
			if company is None or len(company)>1 or len(company)==0 or 'latest_filing_date' not in company:
				logging.error('cik:'+cik+' has no matching company')
				continue
				
			last_filing_pull=self.mongo.db[self.collections['intrinio_pull_times']].find_one({"cik":cik,'collection':self.collections['intrinio_filings']})
			if full_update is not True and last_filing_pull is not None and pd.to_datetime(last_filing_pull['date'])>=last_trade_day:
				logging.info('last_filing_pull:'+str(last_filing_pull['date']))
				logging.info('we are already up to date on this company filings, continueing:'+cik)
				continue
				
			max_filing_date=self.mongo.db[self.collections['intrinio_filings']].find_one({"cik":cik},sort=[("filing_date",-1)])
			if max_filing_date!=None and full_update is not True:
				max_filing_date=(pd.to_datetime(max_filing_date['filing_date'])).date().strftime('%Y-%m-%d')
			else:
				max_filing_date=None
	
			logging.info('max_filing_date:'+str(max_filing_date))					
			filings=self.intrinio.get_company_filings(start_date=max_filing_date,identifier=cik)
			
			if filings is None or len(filings)==0:
				logging.info('filings is none or filings are empty')
			else:
				filings=pd.DataFrame(filings)
				filings['cik']=cik
				filings['_id']=filings['accno']
				filings=filings.sort_values('accepted_date')
				for index,filing in filings.iterrows():
					filing_data=filing.to_dict()
					self.mongo.db[self.collections['intrinio_filings']].update({'accno':filing_data['accno']},filing_data,upsert=True)
				logging.info(str(len(filings))+': filings updated for:'+cik)
			
			logging.info('update filings pull date for cik:'+cik+' to:'+str(last_trade_day))
			pull_data={'cik':cik,'collection':self.collections['intrinio_filings'],'date':last_trade_day.strftime('%Y-%m-%d')}
			self.mongo.db[self.collections['intrinio_pull_times']].update({"cik":cik,'collection':self.collections['intrinio_filings']},pull_data,upsert=True)
		return True
	def update_standardized_fundamentals(self,full_update=False,ciks=None):	

		last_trade_day=self.last_trade_day
		
		self.mongo.create_index(self.collections['intrinio_standardized_fundamentals'],'cik')
		self.mongo.create_index(self.collections['intrinio_standardized_fundamentals'],'fiscal_year')
		self.mongo.create_index(self.collections['intrinio_standardized_fundamentals'],'fiscal_period')
		self.mongo.create_index(self.collections['intrinio_standardized_fundamentals'],'statement_type')
		self.mongo.create_index(self.collections['intrinio_standardized_fundamentals'],'filing_date')

		self.mongo.create_index(self.collections['intrinio_pull_times'],'collection')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'date')
		self.mongo.create_index(self.collections['intrinio_pull_times'],'cik')
		
		if ciks is None: #pass in a list of ciks that we want to update
			ciks=self.cq.existing_ciks()
		for cik in ciks:
			company=self.cq.get_company(cik)
			if company is None or len(company)==0 or 'latest_filing_date' not in company.columns:
				continue
			company_latest_filing_date=(pd.to_datetime(company['latest_filing_date'].iloc[0])).date()
			max_fundamental_date=self.mongo.db[self.collections['intrinio_standardized_fundamentals']].find_one({"cik":cik},sort=[("filing_date",-1)])
			last_fundamental_pull=self.mongo.db[self.collections['intrinio_pull_times']].find_one({"cik":cik,'collection':self.collections['intrinio_standardized_fundamentals']})
	
			if last_fundamental_pull==None:
				if max_fundamental_date==None:					
					last_fundamental_pull=datetime(1900,1,1).date()
				else:
					last_fundamental_pull=(pd.to_datetime(max_fundamental_date['filing_date'])).date()
			else:
				last_fundamental_pull=(pd.to_datetime(last_fundamental_pull['date'])).date()			
			
			if full_update==True: #then we need to re-download everything
				last_fundamental_pull=datetime(1900,1,1).date()
			
			if last_fundamental_pull>company_latest_filing_date:
				logging.info('fundamentals up to date for, not updating:'+cik)
				logging.info('last_fundamental_pull:'+str(last_fundamental_pull))				
				logging.info('company_latest_filing_date:'+str(company_latest_filing_date))
				continue
		
			filings=pd.DataFrame(list(self.mongo.db[self.collections['intrinio_filings']].find({'cik':cik})))
			if len(filings)==0:
				logging.info('no filings found, continueing')
				continue
			filings['filing_date']=pd.to_datetime(filings['filing_date']).dt.date
			filings=filings[filings['filing_date']>=last_fundamental_pull]
			filings=filings[filings['report_type'].isin(['10-K','10-K/A','10-Q','10-Q/A'])]
			filings=filings.sort_values('accepted_date')
			if len(filings)==0:
				logging.info('empty filings after statement type and date filter, no  need to update, continuing')
				continue
			statements_df=pd.DataFrame()
			breakloop=False #if we get a single error, then we dont do the rest to save calls
			for statement_type in ['income_statement','balance_sheet','cash_flow_statement','calculations']: #now we get all the statements
				if breakloop is True:
					continue
				fundamentals=self.intrinio.get_company_fundamentals(identifier=cik,statement=statement_type)
				
				if fundamentals is None or len(fundamentals)==0:
					logging.info('statements returned empty data')
					breakloop=True #so we dont need to get the rest if one has a failure
					
				fundamentals=pd.DataFrame(fundamentals)
				fundamentals['cik']=cik
				fundamentals['statement_type']=statement_type
				statements_df=statements_df.append(fundamentals,ignore_index=True)
				
			if len(statements_df)==0 or breakloop is True:
				logging.info('empty statements_df')
			else:
				statements_df=statements_df.sort_values('filing_date',na_position='first')
				statements_df['filing_date']=pd.to_datetime(statements_df['filing_date'])
				statements_df['start_date']=pd.to_datetime(statements_df['start_date'])
				statements_df['end_date']=pd.to_datetime(statements_df['end_date'])
				statements_df['fiscal_year']=statements_df['fiscal_year'].astype('int')
				statements_df['cik']=statements_df['cik'].astype('str')
				statements_df['fiscal_period']=statements_df['fiscal_period'].astype('str')
				statements_df['statement_type']=statements_df['statement_type'].astype('str')
				
				for index,statement in statements_df.iterrows():
					data=statement.to_dict()
					for item in data:
						if pd.isnull(data[item]):
							data[item]=None
					for item in ['start_date','end_date']:
						if data[item] is not None:
							data[item]=data[item].strftime('%Y-%m-%d')
					for item in ['filing_date']:
						if data[item] is not None:
							data[item]=str(data[item])						
					id='_'.join([str(data['cik']),str(data['fiscal_year']),str(data['fiscal_period']),str(data['statement_type']),str(data['start_date']),str(data['end_date']),str(data['filing_date'])])
					data['_id']=id
					self.mongo.db[self.collections['intrinio_standardized_fundamentals']].update({'_id':id},data,upsert=True)
			logging.info(str(len(statements_df))+': statements updated for:'+cik)
			pull_data={'cik':cik,'collection':self.collections['intrinio_standardized_fundamentals'],'date':last_trade_day.strftime('%Y-%m-%d')}
			self.mongo.db[self.collections['intrinio_pull_times']].update({"cik":cik,'collection':self.collections['intrinio_standardized_fundamentals']},pull_data,upsert=True)
		return True
	def update_standardized_financials(self,full_update=False,ciks=None,fiscal_periods=['FY','Q1','Q2','Q3','Q4']):		
		if ciks is None: #pass in a list of ciks that we want to update
			ciks=self.cq.existing_ciks()
		for cik in ciks:
			
			available_statements=set([x['_id'] for x in self.mongo.db[self.collections['intrinio_standardized_fundamentals']].find({'fiscal_period':{"$in":fiscal_periods},'cik':cik},{"_id":1})])
			downloaded_statements=set([x['_id'] for x in self.mongo.db[self.collections['intrinio_standardized_financials']].find({"_id":{"$in":list(available_statements)}},{"_id":1})])
			bad_statement_ids=set([x['_id'] for x in list(self.mongo.db[self.collections['intrinio_standardized_fundamentals_bad_pull_statements']].find({}))]) #the list of bad statements, so we dont keep trying to pull them each time.
			
			if full_update==True:
				statements_to_download=available_statements
			else:
				statements_to_download=available_statements-downloaded_statements
				statements_to_download=statements_to_download-bad_statement_ids
				
			logging.info(str(len(available_statements))+" available statements for:"+cik)
			logging.info(str(len(downloaded_statements))+" downloaded statements for:"+cik)
			logging.info(str(len(statements_to_download))+" to downloaded statements for:"+cik)
			if len(statements_to_download)==0:
				logging.info('no statements to download for:'+cik)
				continue
			statements_to_download=pd.DataFrame(list(self.mongo.db[self.collections['intrinio_standardized_fundamentals']].find({'_id':{"$in":list(statements_to_download)}})))
			statements_to_download=statements_to_download[statements_to_download['fiscal_period'].isin(fiscal_periods)]
			statements_to_download=statements_to_download.sort_values('end_date')
			for index,statement_to_download in statements_to_download.iterrows():
				data=statement_to_download.to_dict()
				id=data["_id"]
				resp=self.intrinio.get_company_financials(identifier=cik,statement=data['statement_type'],fiscal_year=data['fiscal_year'],fiscal_period=data['fiscal_period'])
				if resp is None or len(resp)==0:
					logging.error('empty resp for statement id:'+str(id))
					self.mongo.db[self.collections['intrinio_standardized_fundamentals_bad_pull_statements']].update({'_id':id},{"_id":id,"id":id},upsert=True)
					logging.error('bad statement id added')
					continue
				newdata={'_id':id}
				resp=pd.DataFrame(resp)
				for index,row in resp.iterrows():
					newdata[row['tag']]=row['value']
				self.mongo.db[self.collections['intrinio_standardized_financials']].update({'_id':id},newdata,upsert=True)
			logging.info(str(len(statements_to_download))+': statement data updated for:'+cik)
		return True
	def update_standardized_tags_and_labels(self):
	
		self.mongo.create_index(self.collections['intrinio_standardized_tags_and_labels'],'statement_type')
		self.mongo.create_index(self.collections['intrinio_standardized_tags_and_labels'],'template')

		tagsdf=pd.DataFrame()
		for statement_type in ['income_statement','balance_sheet','cash_flow_statement','calculations']:
			for type in ['industrial','financial']:
				result=self.intrinio.get_standardized_tags(statement=statement_type,template=type)
				if result is None or len(result)==0:
					continue
				result=pd.DataFrame(result)
				result['statement_type']=statement_type
				result['template']=type
				tagsdf=tagsdf.append(result)
		for index,item in tagsdf.iterrows():
			data=item.to_dict()
			keys=data.keys()
			keys.sort()
			id='_'.join([str(data[key]) for key in keys])
			data['_id']=id
			self.mongo.db[self.collections['intrinio_standardized_tags_and_labels']].update({'_id':id},data,upsert=True)
		logging.info('tags updated')
		return
	def update_historical_data(self,full_update=False,ciks=None,items=None):
	
		self.mongo.create_index(self.collections['historical_data'],'item')
		self.mongo.create_index(self.collections['historical_data'],'cik')
		self.mongo.create_index(self.collections['historical_data'],'date')
		
		if ciks is None: #pass in a list of ciks that we want to update
			ciks=self.cq.existing_ciks()
		for cik in ciks:
			for item in items:
				max_date=self.mongo.db[self.collections['historical_data']].find_one({'cik':cik,'item':item},sort=[("date",-1)])
				if max_date==None or full_update==True:
					max_date=None
				else:
					max_date=max_date['date']
				resp=self.intrinio.get_historical_data(identifier=cik,item=item,start_date=max_date)
				if resp is None or len(resp)==0:
					continue
				resp=pd.DataFrame(resp)
				resp=resp.sort_values('date')
				resp['cik']=cik
				resp['item']=item
				for index,row in resp.iterrows():
					data=row.to_dict()
					keys=data.keys()
					keys.sort()
					id='_'.join([str(data[key]) for key in keys])
					data['_id']=id
					self.mongo.db[self.collections['historical_data']].update({'_id':id},data,upsert=True)
		return True

	def update_exchange_prices(self,exch_symbols=None):
		bad_figis=set([x['_id'] for x in list(self.mongo.db[self.collections['intrinio_bad_figis']].find({}))]) #the list of bad figis, so we dont keep trying to pull them each time.

		price_days=self.cq.get_market_days(relativedelta(weeks=2))
		last_complete_day=price_days[-1]
		day_before_last_complete_day=price_days[-2]
		
		if exch_symbols is None:
			exch_symbols=[x['symbol'] for x in list(self.mongo.db[self.collections['intrinio_exchanges']].find({},{"symbol":1,"_id":0}))]
		for symbol in exch_symbols:
			exchange=self.mongo.db[self.collections['intrinio_exchanges']].find_one({'symbol':symbol})
			exchange_symbol=exchange['symbol']
			data=self.intrinio.get_exchange_prices(identifier=exchange_symbol,price_date=last_complete_day)
			if data is None or len(data)==0:
				logging.error('no mic prices')
				return		
			data=pd.DataFrame(data)
			data=data[pd.notnull(data['figi'])] #get rid of items with no figi
			for x in ['open','high','low','close','volume','ex_dividend','split_ratio','adj_open','adj_high','adj_low','adj_close','adj_volume']:
				if x in data.columns:
					data[x]=data[x].astype('float')
			data=data[pd.notnull(data['ticker'])]
			data=data[pd.notnull(data['figi'])]
			data=data[~(data['figi'].isin(list(bad_figis)))]#remove any of the bad figis
			
			for row,security_day in data.iterrows():
				logging.info('processing day info for figi:'+security_day['figi'])
				security_day=security_day.to_dict()
				# logging.info(exchange['mic'])
				# logging.info(security_day['figi'])
				# we had to remove also filtering by MIC because now the prices endpoint returns the common eOD prices, regardless of mic
				
				company=self.mongo.db[self.collections['intrinio_companies']].find_one({'securities':{'$elemMatch':{'figi':security_day['figi']}}})
				if company is None or company['ticker']!=security_day['ticker']:
					logging.info('no company found for figi:'+security_day['figi'])
					company_update_result=self.update_company(security_day['ticker'])
					if company_update_result is False: #if the company update was not sucessfull
						self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':security_day['figi']},{"_id":security_day['figi'],"id":security_day['figi']},upsert=True)
				
				security=self.mongo.db[self.collections['intrinio_securities']].find_one({'figi':security_day['figi']}) #try to find a matching security
				if security is None or security['ticker']!=security_day['ticker'] or security['figi_ticker']!=security_day['figi_ticker']:
					logging.info('no security found for figi:'+security_day['figi'])
					security_update_result=self.update_security(figi=security_day['figi'])
					if security_update_result is False: #if the company update was not sucessfull
						self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':security_day['figi']},{"_id":security_day['figi'],"id":security_day['figi']},upsert=True)					
				
				current_prices=self.cq.get_intrinio_prices(security_day['figi'],'figi')
				if current_prices is not None and len(current_prices)>0 and pd.to_datetime(current_prices['date']).max()>=pd.to_datetime(last_complete_day):
					logging.info('we are already up to date for figi:'+security_day['figi'])
					id='_'.join([security_day['figi'],security_day['date']])
					security_day['_id']=id
					self.mongo.db[self.collections['intrinio_prices']].update({"_id":id},security_day,upsert=True)
				elif current_prices is None:
					logging.info('need to redownload everything for figi:'+security_day['figi'])
					logging.info('current prices is None')
					self.update_figi_prices(security_day['figi'],full_update=True)
				elif len(current_prices)==0:
					logging.info('need to redownload everything for figi:'+security_day['figi'])
					logging.info('current prices length is 0')
					self.update_figi_prices(security_day['figi'],full_update=True)
				elif security_day['ex_dividend']!=0:
					logging.info('need to redownload everything for figi:'+security_day['figi'])
					logging.info('dividend is not 0:'+str(security_day['ex_dividend']))
					self.update_figi_prices(security_day['figi'],full_update=True)
				elif security_day['split_ratio']!=1:
					logging.info('need to redownload everything for figi:'+security_day['figi'])
					logging.info('split ratio is not 1:'+str(security_day['split_ratio']))
					self.update_figi_prices(security_day['figi'],full_update=True)
				elif len(current_prices)>0 and pd.to_datetime(current_prices['date']).max()>=pd.to_datetime(day_before_last_complete_day):
					logging.info('only updating one day for for figi:'+security_day['figi'])
					id='_'.join([security_day['figi'],security_day['date']])
					security_day['_id']=id
					self.mongo.db[self.collections['intrinio_prices']].update({"_id":id},security_day,upsert=True)
				elif len(current_prices)>0 and pd.to_datetime(current_prices['date']).max()<pd.to_datetime(day_before_last_complete_day):
					logging.info('updating multiple days for figi:'+security_day['figi'])
					self.update_figi_prices(security_day['figi'])
				else:
					logging.error('unknown condition, check this, it should never happen for figi:'+security_day['figi'])
		return
	def update_index_prices(self,index,full_update=False):
		self.mongo.create_index(self.collections['intrinio_prices'],'ticker')
		self.mongo.create_index(self.collections['intrinio_prices'],'date')
		
		price_days=self.cq.get_market_days(relativedelta(weeks=2))
		price_date=price_days[-1]
		previous_price_day=price_days[-2]
		
		if full_update is True:
			self.mongo.db[self.collections['intrinio_prices']].remove({'ticker':index})

		current_prices=self.cq.get_intrinio_index_prices(index)
		if current_prices is None or len(current_prices)==0:
			start_date=None
		else:
			start_date=current_prices['date'].max()
			if start_date==price_date:
				logging.info('already up to date for index:'+index)
				return
		prices_to_add=self.intrinio.get_prices(identifier=index,start_date=start_date,end_date=price_date)
		if prices_to_add is None or len(prices_to_add)==0:
			return
		prices_to_add=pd.DataFrame(prices_to_add)
		prices_to_add['ticker']=index
		prices_to_add=prices_to_add.sort_values('date')
		for row,price_row in prices_to_add.iterrows():
			price_row=price_row.to_dict()
			id='_'.join([price_row['ticker'],price_row['date']])
			price_row['_id']=id
			self.mongo.db[self.collections['intrinio_prices']].update({"_id":id},price_row,upsert=True)
		logging.info('index updated:'+index)	
		return
	def update_security(self,figi):
		self.mongo.create_index(self.collections['intrinio_securities'],'figi')
		self.mongo.create_index(self.collections['intrinio_securities'],'ticker')
		self.mongo.create_index(self.collections['intrinio_securities'],'stock_exchange')
		self.mongo.create_index(self.collections['intrinio_securities'],'mic')
		
		security_data=self.intrinio.get_securities(identifier=figi)
		if security_data is None:
			return False
		security_data['_id']=security_data['figi']
		exchange=self.mongo.db[self.collections['intrinio_exchanges']].find_one({'symbol':security_data['exch_symbol']}) #because no matter what you put in, it will always return the same thing
		if exchange is None:
			logging.error('unknown exchange for figi:'+figi)
			return
		security_data['mic']=exchange['mic']
		self.mongo.db[self.collections['intrinio_securities']].update({"_id":security_data['_id']},security_data,upsert=True)
		return True
	def update_exchanges(self):
		exchanges=self.intrinio.get_exchanges()
		if exchanges is None:
			return
		exchanges=pd.DataFrame(exchanges)
		for row,exchange in exchanges.iterrows():
			exchange=exchange.to_dict()
			exchange['_id']=exchange['mic']
			id=exchange['_id']
			self.mongo.db[self.collections['intrinio_exchanges']].update({"_id":id},exchange,upsert=True)
		return
	def update_securities(self,exch_symbols=None):
		#exch_symbols should be  a list of exchange symbols we want to get and update securities for
		self.mongo.create_index(self.collections['intrinio_securities'],'figi')
		self.mongo.create_index(self.collections['intrinio_securities'],'ticker')
		self.mongo.create_index(self.collections['intrinio_securities'],'stock_exchange')
		self.mongo.create_index(self.collections['intrinio_securities'],'mic')
		
		if exch_symbols is None:
			exch_symbols=[x['symbol'] for x in list(self.mongo.db[self.collections['intrinio_exchanges']].find({},{"symbol":1,"_id":0}))]
		for symbol in exch_symbols:
			exchange=self.mongo.db[self.collections['intrinio_exchanges']].find_one({'symbol':symbol})
			if exchange is None:
				logging.error('no exchange found for:'+symbol)
			securities=self.intrinio.get_securities(exch_symbol=symbol)
			if securities is None or len(securities)==0:
				continue
			securities=pd.DataFrame(securities)
			securities['mic']=exchange['mic']
			securities=securities[pd.notnull(securities['figi'])]
			securities=securities[pd.notnull(securities['ticker'])]
			securities=securities.sort_values('last_crsp_adj_date') #sometimes intrinio does not replace old tickers
			securities=securities.drop_duplicates('figi',keep='last')
			for row,security in securities.iterrows():
				security=security.to_dict()
				logging.info('updating security:'+security['figi'])
				security['_id']=security['figi']
				self.mongo.db[self.collections['intrinio_securities']].update({"_id":security['_id']},security,upsert=True)
		return
	def update_all_figi_prices(self,full_update=False):
		allfigis=list(set([x['_id'] for x in list(self.mongo.db[self.collections['intrinio_securities']].find({},{'_id':1}))]))
		for figi in allfigis:
			self.update_figi_prices(figi,full_update)
		logging.info('all figis updated')
		return
	def update_figi_prices(self,figi,full_update=False):
		bad_figis=set([x['_id'] for x in list(self.mongo.db[self.collections['intrinio_bad_figis']].find({}))]) #the list of bad figis, so we dont keep trying to pull them each time.
		if figi in bad_figis:
			logging.error(figi+' is a bad figi, we will not update the prices')
			return
		self.mongo.create_index(self.collections['intrinio_prices'],'date')
		self.mongo.create_index(self.collections['intrinio_prices'],'figi')
		
		logging.info('updating figi prices for:'+figi)
		last_price_day=self.cq.get_last_complete_market_day()
		current_prices=self.cq.get_intrinio_prices(figi,'figi')
		if full_update is True or current_prices is None or len(current_prices)==0:
			self.mongo.db[self.collections['intrinio_prices']].remove({'figi':figi})
			newprices=self.intrinio.get_prices(identifier=figi,end_date=last_price_day)
			if newprices is None or len(newprices)==0:
				logging.info('bad figi:'+figi)
				self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':figi},{"_id":figi,"id":figi},upsert=True)
				return

		else:
			max_existing_date=(pd.to_datetime(current_prices['date']).max()).strftime('%Y-%m-%d')
			if max_existing_date==last_price_day:
				logging.info('figi is already up to date:'+figi)
				return
			logging.info('max date for figi:'+figi+' is:'+max_existing_date)
			newprices=self.intrinio.get_prices(identifier=figi,start_date=max_existing_date,end_date=last_price_day)
			if newprices is None or len(newprices)==0:
				logging.info('bad figi, returned no data:'+figi)
				self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':figi},{"_id":figi,"id":figi},upsert=True)
				return

			tempnewprices=pd.DataFrame(newprices)
			for x in ['open','high','low','close','volume','ex_dividend','split_ratio','adj_open','adj_high','adj_low','adj_close','adj_volume']:
				if x in tempnewprices.columns:
					tempnewprices[x]=tempnewprices[x].astype('float')
				
			if (tempnewprices['ex_dividend']!=0).any() or (tempnewprices['split_ratio']!=1).any():
				logging.info('need to redownload everything for figi:'+figi+' because of a bad dividend or split ratio')
				self.mongo.db[self.collections['intrinio_prices']].remove({'figi':figi}) #remove what we already ahve
				newprices=self.intrinio.get_prices(identifier=figi,end_date=last_price_day)
				if newprices is None or len(newprices)==0:
					logging.info('bad figi, returned no data:'+figi)
					self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':figi},{"_id":figi,"id":figi},upsert=True)
					return
		if newprices is None or len(newprices)==0:
			logging.info('bad figi, returned no data:'+figi)
			self.mongo.db[self.collections['intrinio_bad_figis']].update({'_id':figi},{"_id":figi,"id":figi},upsert=True)
			return
			
		newprices=pd.DataFrame(newprices)
		for x in ['open','high','low','close','volume','ex_dividend','split_ratio','adj_open','adj_high','adj_low','adj_close','adj_volume']:
			if x in newprices.columns:
				newprices[x]=newprices[x].astype('float')
			
		newprices=newprices.sort_values('date')
		newprices['figi']=figi
		newprices=newprices.drop_duplicates('date')
		for index,price_row in newprices.iterrows():
			price_row=price_row.to_dict()
			id='_'.join([price_row['figi'],price_row['date']])
			price_row['_id']=id
			self.mongo.db[self.collections['intrinio_prices']].update({"_id":id},price_row,upsert=True)
		logging.info(str(len(newprices))+' prices updated')
		logging.info('security prices updated:'+figi)
		return True
if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	i=IntrinioUpdater(config_file='finance_cfg.cfg')
	