import warnings
warnings.filterwarnings("ignore")
import logging
import inspect
import commonqueries
import quantvaluedata
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import configwrapper
import os


class metrics():
	def __init__(self,config_file='finance_cfg.cfg'):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections

		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)
		self.q = quantvaluedata.quantvaluedata(self.cq.get_all_tags())

		self.last_valid_day=pd.to_datetime(self.cq.get_last_complete_market_day())
		self.valid_figis=[x['figi'] for x in list(self.cq.mongo.db[collections['intrinio_prices']].find({'figi':{'$exists':True},'date':{"$gte":self.last_valid_day.date().strftime("%Y-%m-%d")}}))]
		self.spyprices=self.cq.get_intrinio_prices(value='BBG000BDTBL9',key='figi') #because sometimes the index does not update in time...
		if pd.to_datetime(self.spyprices['date'].max()) != self.last_valid_day:
			logging.error('our SPY prices are not up to date')
			exit()

		companies=self.cq.get_companies()
		self.companies=companies
		self.companies=self.companies[pd.to_datetime(self.companies['latest_filing_date']).dt.date>=datetime.now().date()-relativedelta(months=6)]
		self.companies=self.companies[self.companies['standardized_active']==True]
		self.companies=self.companies[self.companies['ticker'].isin(self.get_valid_instrument_symbols())]
		self.companies['figi']=self.companies['ticker'].apply(self.cq.ticker2figi)
		self.companies=self.companies[self.companies['figi'].isin(self.valid_figis)]

		self.total_market_cap=self.get_total_market_cap()

		return
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def get_valid_instrument_symbols(self):
		instruments = self.cq.get_robinhood_instruments()
		instruments = instruments[instruments['tradeable'] == True]
		valid_instrument_symbols = instruments['symbol'].unique()
		return valid_instrument_symbols
	def get_total_market_cap(self):
		total_market_cap=0
		for index,company in self.companies.iterrows():
			cik=company['cik']
			total_market_cap+=self.cq.get_last_market_cap2(cik=cik)
		return total_market_cap

	def update_all_metrics(self):
		i=0
		totallen = len(self.companies)
		for index,company in self.companies.iterrows():
			i+=1
			logging.info('updating:' + company['cik'] + " percent complete of update:" + str(float(i) / totallen))
			self.update_company_metrics(company['ticker'])
		self.cq.mongo.db[self.collections['metrics']].remove({'lastpriceday':{"$lt":self.last_valid_day.date().strftime("%Y-%m-%d")}}) #remove anyone that is notup to date

	def update_company_metrics(self,ticker,full_update=False):
		company=self.cq.get_company(cik=self.cq.ticker2cik(ticker))
		if len(company)>1  or len(company)==0 or company is None:
			logging.error( "company not found for metrics:"+str(company))
			return
		company=company.iloc[0]
		cik=company['cik']
		ticker=company['ticker']

		existing_data = self.cq.mongo.db[self.collections['metrics']].find_one({'_id': cik})
		if not full_update and existing_data is not None and 'lastpriceday' in existing_data and pd.to_datetime(existing_data['lastpriceday'])==pd.to_datetime(self.last_valid_day):
			logging.info('we are already up to date on metrics')
			return
		statements_df=self.cq.get_fixed_quarter_df(cik)
		prices_df=self.cq.get_intrinio_prices(ticker,key='ticker')
		if statements_df is None or prices_df is None or len(statements_df)==0 or len(prices_df)==0:
			logging.info('empty statements or prices')
			return existing_data

		logging.info('calculing newdata')

		newdata={}
		newdata["_id"]=cik
		#things that require prices
		newdata['emyield']=self.q.get_emyield(statements_df,prices_df,seed=-1,length=4)
		newdata['5yearemyield']=self.q.get_emyield(statements_df,prices_df,seed=-1,length=20)
		newdata['marketcap']=self.q.get_market_cap(statements_df,prices_df,seed=-1)
		newdata['price']=prices_df['adj_close'].iloc[-1]
		newdata['momentum']=self.q.get_momentum(prices_df)
		newdata['fip']=self.q.get_fip(prices_df)
		newdata['pfd']=self.q.get_pfd(statements_df,prices_df,self.spyprices,self.total_market_cap,seed=-1,days=90)
		newdata['lastpriceday']=pd.to_datetime(prices_df['date'].max()).strftime('%Y-%m-%d')
		last_statement_date=pd.to_datetime(statements_df['end_date'].max())
		prices_df_copy=prices_df.copy(deep=True)
		prices_df_copy['date']=pd.to_datetime(prices_df_copy['date'])
		prices_df_copy=prices_df_copy[prices_df_copy['date']>=last_statement_date]
		split_since_last_statement=False
		if (prices_df_copy['split_ratio'].astype('float')!=1).any():
			split_since_last_statement=True
		newdata['split_since_last_statement'] = split_since_last_statement
		newdata['cik']=company['cik']
		newdata['ticker']=company['ticker']
		newdata['symbol']=company['ticker']
		newdata['name']=company['name']
		newdata['sector']=company['sector']
		newdata['industry_category']=company['industry_category']
		newdata['industry_group']=company['industry_group']
		newdata['sec13']=self.cq.mongo.db[self.collections['intrinio_filings']].find({"report_type":{"$in":["SC 13G","SC 13D"]},"cik":company['cik'],"filing_date":{"$gte":(datetime.today().date()-relativedelta(years=1)).strftime('%Y-%m-%d')}}).count()

		release_date=self.cq.mongo.db[self.collections['robinhood_earnings']].find({'cik':company['cik'],"date":{"$gt":statements_df['end_date'].max()}},{"date":1,"_id":0}).sort("date",1).limit(1)
		if release_date is None or release_date.count()==0:
			newdata['release_date']=None
		else:
			newdata['release_date']=release_date[0]['date']
			if pd.to_datetime(newdata['release_date'])>pd.to_datetime(statements_df['end_date'].max())+relativedelta(months=4): #if the release is more than 1 full quarter after the end date...
				newdata['release_date']=None

		if release_date is None:
			next_release=None
		else:
			next_release=self.cq.mongo.db[self.collections['robinhood_earnings']].find({'cik':company['cik'],"date":{"$gt":newdata['release_date']}},{"date":1,"_id":0}).sort("date",1).limit(1)
		if next_release is None or next_release.count()==0:
			newdata['next_release']=None
		else:
			newdata['next_release']=next_release[0]['date']
			#if the next release is more than 5 months away from the release date, then make it done, there is clrealy a problem somewhere
			if pd.to_datetime(newdata['next_release'])>pd.to_datetime(newdata['release_date'])+relativedelta(months=4):
				newdata['next_release'] = None

		newdata['valid_trade']=False #determine if we are even allowed to trade this stock
		if newdata['next_release'] is not None and newdata['release_date'] is not None and pd.to_datetime(newdata['lastpriceday'])>=pd.to_datetime(self.last_valid_day) and pd.to_datetime(newdata['next_release'])>=pd.to_datetime(self.last_valid_day) and pd.to_datetime(newdata['release_date'])<=pd.to_datetime(self.last_valid_day) and pd.to_datetime(newdata['next_release'])<=pd.to_datetime(self.last_valid_day)+relativedelta(months=4) and pd.to_datetime(newdata['release_date'])>=pd.to_datetime(self.last_valid_day)-relativedelta(months=4):
			newdata['valid_trade']=True

		iex_short_interest=self.cq.mongo.db[self.collections['iex_stats']].find_one({'symbol':ticker,'shortDate':{"$gte":(self.last_valid_day-relativedelta(years=1)).strftime('%Y-%m-%d')}})
		if iex_short_interest is None or 'shortRatio' not in iex_short_interest or pd.isnull(iex_short_interest['shortRatio']):
			newdata['daystocover']=0
		else:
			newdata['daystocover'] = float(iex_short_interest['shortRatio'])

		existing_data=self.cq.mongo.db[self.collections['metrics']].find_one({'_id':cik})
		if existing_data is None or ('end_date' in existing_data and existing_data['end_date']!=statements_df['end_date'].max()):
			matchfound=False
		else:
			matchfound=True
			logging.info('match found, can pull from already existing data')

		#things that we should NOT reget if we have already
		if existing_data is not None and 'last_form4_date' in existing_data and existing_data['last_form4_date']==self.cq.last_form4_date(cik):
			newdata['insider_purchase_ratio']=existing_data['insider_purchase_ratio']
			newdata['last_form4_date']=existing_data['last_form4_date']
		else:
			totalbuys=self.cq.get_total_purchase_value_for_insiders(cik,period=relativedelta(years=1)) #see who has purchased raw stock in the last year
			if pd.notnull(newdata['marketcap']) and pd.notnull(totalbuys):
				newdata['insider_purchase_ratio']=totalbuys/newdata['marketcap']
				newdata['last_form4_date']=self.cq.last_form4_date(cik)
			else:
				newdata['insider_purchase_ratio']=None
				newdata['last_form4_date']=self.cq.last_form4_date(cik)


		if matchfound is True and 'snoa' in existing_data:
			newdata['snoa']=existing_data['snoa']
		else:
			newdata['snoa']=self.q.get_scalednetoperatingassets(statements_df,seed=-1)

		if matchfound is True and 'sta' in existing_data:
			newdata['sta']=existing_data['sta']
		else:
			newdata['sta']=self.q.get_scaledtotalaccruals(statements_df,seed=-1,length=4)

		if matchfound is True and 'end_date' in existing_data:
			newdata['end_date']=existing_data['end_date']
		else:
			newdata['end_date']=statements_df['end_date'].max()

		if matchfound is True and 'ms' in existing_data:
			newdata['ms']=existing_data['ms']
		else:
			newdata['ms']=self.q.get_marginstability(statements_df,seed=-1,length1=20,length2=4) #go back 5 years

		if matchfound is True and 'mg' in existing_data:
			newdata['mg']=existing_data['mg']
		else:
			newdata['mg']=self.q.get_margingrowth(statements_df,seed=-1,length1=20,length2=4) #go back 5 years

		if matchfound is True and 'cfoa' in existing_data:
			newdata['cfoa']=existing_data['cfoa']
		else:
			newdata['cfoa']=self.q.get_cashflowonassets(statements_df,seed=-1,length1=20,length2=4)

		if matchfound is True and 'roa' in existing_data:
			newdata['roa']=existing_data['roa']
		else:
			newdata['roa']=self.q.get_longtermroa(statements_df,seed=-1,length1=20,length2=4)

		if matchfound is True and 'roc' in existing_data:
			newdata['roc']=existing_data['roc']
		else:
			newdata['roc']=self.q.get_longtermroc(statements_df,seed=-1,length1=20,length2=4)

		if matchfound is True and 'pman' in existing_data:
			newdata['pman']=existing_data['pman']
		else:
			newdata['pman']=self.q.get_pman(statements_df,seed1=-1,seed2=-5,length=4)

		if matchfound is True and 'fs' in existing_data:
			newdata['fs']=existing_data['fs']
		else:
			newdata['fs']=self.q.get_financialstrength(statements_df,seed1=-1,seed2=-5,length=4)

		if matchfound is True and 'shareissuanceratio' in existing_data:
			newdata['shareissuanceratio']=existing_data['shareissuanceratio']
		else:
			newdata['shareissuanceratio']=self.q.get_shareissuanceratio(statements_df,seed1=-1,seed2=-5,length1=4,length2=4) #we want this to be less than1

		if matchfound is True and 'newshares' in existing_data:
			newdata['newshares']=existing_data['newshares']
		else:
			newdata['newshares']=self.q.get_neqiss(statements_df,seed=-1,length=4)

		if matchfound is True and 'tatl' in existing_data:
			newdata['tatl']=existing_data['tatl']
		else:
			newdata['tatl']=self.q.get_tatl(statements_df,seed=-1)

		if matchfound is True and 'cacl' in existing_data:
			newdata['cacl']=existing_data['cacl']
		else:
			newdata['cacl']=self.q.get_cacl(statements_df,seed=-1)

		logging.info(newdata)
		self.cq.mongo.db[self.collections['metrics']].update({'cik':cik},newdata,upsert=True)
		return newdata

def main(config_file='finance_cfg.cfg'):
	m=metrics(config_file=config_file)
	m.update_all_metrics()


if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	main(config_file='finance_cfg.cfg')
