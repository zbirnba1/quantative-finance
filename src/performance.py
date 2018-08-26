import googlesheetuploader
import logging
import commonqueries
import pandas as pd
import robinhoodwrapper
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import inspect
import configwrapper
import os

class Performance():
	def __init__(self,config_file=None):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		data_collections=self.build_collections('FINANCIALDATA_COLLECTIONS')
		user_collections=self.build_collections('USERS_COLLECTIONS')
		self.data_collections=data_collections
		self.user_collections=user_collections
		self.data_cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=data_collections)
		self.user_cq=commonqueries.CommonQueries(port=self.config.get_int('USERS_MONGO','port'),host=self.config.get_string('USERS_MONGO','host'), username=self.config.get_string('USERS_MONGO','username'), password=self.config.get_string('USERS_MONGO','password'), dbname=self.config.get_string('USERS_MONGO','dbname'),collections=user_collections)
		self.key_file=self.config.get_string('GOOGLE_CLOUD','service_file') #get the google key file
		return
	def build_collections(self,section='FINANCIALDATA_COLLECTIONS'):
		self.user_collections={}
		for option in self.config.get_options(section):
			self.user_collections[option]=self.config.get_string(section,option)
		return self.user_collections
		
	def get_performance(self,rh,spyprices):
		spyprices['date']=pd.to_datetime(spyprices['date'])
		history=rh.get_portfolio_history(span='all',interval=None,bounds=None,account=None)
		history['begins_at']=pd.to_datetime(history['begins_at'])
		acountstart=pd.to_datetime(rh.get_accounts()[0]['created_at'])
		history=history[history['begins_at']>=acountstart]
		for index,row in history.iterrows():
			startdate=row['begins_at']
			tempfd=spyprices[spyprices['date']==startdate]
			if len(tempfd)==0:
				continue
			price=tempfd['adj_close'].iloc[0]
			#history.set_value(index,'start_price',float(price))
			history.loc[index,'start_price']=float(price)
		history['begins_at']=history['begins_at'].dt.date
		first_day=pd.to_datetime(history['begins_at'].min())
		for index,row in history.iterrows():
			history.loc[index,'num_days']=int((pd.to_datetime(row['begins_at'])-first_day).days)
		return history
	def update_performance_spreadsheets(self):
		spyprices=self.data_cq.get_intrinio_prices(value='BBG000BDTBL9',key='figi') #because sometimes the index does not update in time...
		if pd.to_datetime(spyprices['date'].max()) != pd.to_datetime(self.data_cq.get_last_complete_market_day()):
			logging.error('our SPY prices are not up to date')
			return

		recommendeddf=pd.DataFrame(list(self.data_cq.mongo.db[self.data_collections['quantative_value_recommended']].find({},{"_id":0})))
		newlist=['ticker']
		newlist=newlist+list(set(recommendeddf.columns)-set(newlist))#moves the ticker column to the front
		recommendeddf=recommendeddf[newlist]

		#calculate some macro indactors for everyone
		industrialproduction = self.data_cq.get_fred_df('INDPRO')
		realretailsales = self.data_cq.get_fred_df('RRSFS')
		nonfarmemployment = self.data_cq.get_fred_df('PAYEMS')
		realpersonalincome = self.data_cq.get_fred_df('DSPIC96')

		industrialproduction = (industrialproduction['value'].iloc[-1] / industrialproduction['value'].max()) - 1
		nonfarmemployment = (nonfarmemployment['value'].iloc[-1] / nonfarmemployment['value'].max()) - 1
		realretailsales = (realretailsales['value'].iloc[-1] / realretailsales['value'].max()) - 1
		realpersonalincome = (realpersonalincome['value'].iloc[-1] / realpersonalincome['value'].max()) - 1
		RI = np.mean([industrialproduction, nonfarmemployment, realretailsales, realpersonalincome])  # the recission indicator
		p0 = spyprices[pd.to_datetime(spyprices['date']).dt.date >= (datetime.now().date() - relativedelta(years=1))]['adj_close'].iloc[0]
		p1 = spyprices['adj_close'].iloc[-1]
		sp_return = float(p1) / float(p0) - 1
		movingaverage = spyprices['adj_close'].rolling(200).mean().iloc[-1]
		current = spyprices['adj_close'].iloc[-1]
		fed_10yr = float(self.data_cq.get_fred_df('DGS10').iloc[-1]['value']) / 100
		fed_1yr = float(self.data_cq.get_fred_df('DGS1').iloc[-1]['value']) / 100

		macroindicators=pd.Series()
		macroindicators['Fed 1 YR']=fed_1yr
		macroindicators['S&P 1 year return']=sp_return
		macroindicators['S&P current']=current
		macroindicators['S&P 200 day avg']=movingaverage
		macroindicators['Recession Indcators off high']=RI
		macroindicators['10 yr, 1 yr spread']=float(fed_10yr)-float(fed_1yr)
		market_type="BULL"
		if (float(fed_10yr)-float(fed_1yr)<=0) or sp_return<=fed_1yr or current<=movingaverage or RI<=-.0093:
			market_type="BEAR"
		macroindicators["Market_Type"]=market_type
		macroindicators['date'] = pd.to_datetime(spyprices['date'].max()).date().strftime('%Y-%m-%d')

		user_df=pd.DataFrame(list(self.user_cq.mongo.db[self.user_collections['robinhood_users']].find()))
		user_df=user_df.sort_values('username')
		user_df=user_df.drop_duplicates('username') #has the usernames and passwords of all robinhood users
		for index,account in user_df.iterrows():
			if 'googlesheetid' not in account or account['googlesheetid'] is None or not googlesheetuploader.pgs_is_valid_spreadsheet(account['googlesheetid'],service_file=self.key_file):
				account['googlesheetid']=googlesheetuploader.create_new_sheet('stock_performance',keyfile=self.key_file)
				self.user_cq.mongo.db[self.user_collections['robinhood_users']].update({"_id":account["_id"]},account.to_dict())

			perms=googlesheetuploader.list_permissions(account['googlesheetid'],keyfile=self.key_file)
			if len(perms[(perms['emailAddress'].astype('str')==account['email']) & (perms['role'].astype('str')=='writer')])==0:
				googlesheetuploader.add_permission(id=account['googlesheetid'],email=account['email'],notify=False,perm_type='user',role='writer',keyfile=self.key_file)

			rh_user=robinhoodwrapper.RobinHoodWrapper(username=account['username'],password=account['password'],instruments=self.data_cq.get_robinhood_instruments())
			history=self.get_performance(rh_user,spyprices)
			history['portfolio_change']=(history['adjusted_close_equity'].astype(float)/history['adjusted_close_equity'].astype(float).iloc[0])-1
			history['index_change']=(history['start_price']/history['start_price'].iloc[0])-1
			history['over_performance']=history['portfolio_change']-history['index_change']

			s=pd.notnull(history).all(axis=1)
			history=history[s]
			history['begins_at']=pd.to_datetime(history['begins_at']).dt.strftime('%Y-%m-%d')
			orig_history=history.copy(deep=True)
			history=history[['begins_at','portfolio_change','index_change','over_performance']]
			googlesheetuploader.pgs_upload_df_to_worksheet(history,account['googlesheetid'],'performance',service_file=self.key_file)
			history=orig_history
			positions=rh_user.get_positions()
			positions['value']=positions['quantity'].astype('float')*positions['last_trade_price'].astype('float')
			positions['weight']=positions['value']/float(rh_user.get_portfolio()['market_value'])
			positions['percent_gain']=positions['last_trade_price'].astype('float')/positions['average_buy_price'].astype('float')-1
			positions['ticker']=positions['symbol']
			positions['cik']=positions['ticker'].apply(self.data_cq.ticker2cik)
			positions['name']=positions['cik'].apply(self.data_cq.get_company_name)
			for index,position in positions.iterrows(): #add the EM percentage
				x=self.data_cq.get_percent_greater_than(self.data_collections['metrics'],self.data_cq.ticker2cik(position['ticker']),'emyield')
				if pd.isnull(x):
					positions.loc[index,'empercentage']=None
				else:
					positions.loc[index,'empercentage']=1-float(x)

			positions=positions[['ticker','name','average_buy_price','last_trade_price','percent_gain','weight']]
			positions=positions.sort_values('weight',ascending=False)

			#we currently dont actually use any of the properties
			cell_properties=pd.DataFrame()
			for index,position in positions.iterrows():
				if float(position['percent_gain'])>0:
					cell_properties.loc[index,'percent_gain']=[{'color':(1.0,0.0,1.0,1.0)}]
				else:
					cell_properties.loc[index, 'percent_gain'] = [{'color': (.5, 0.0, .3, 0.0)}]

			googlesheetuploader.pgs_upload_df_to_worksheet(positions,account['googlesheetid'],'positions',cell_properties=pd.DataFrame(),service_file=self.key_file)

			#upload the desired portfolio
			googlesheetuploader.pgs_upload_df_to_worksheet(recommendeddf,account['googlesheetid'],'recommended_portfolio',service_file=self.key_file)

			#upload some of the macro indicators
			macroindicators['CAGR Portfolio to S&P']=((float(history['over_performance'].iloc[-1])+1)**(float(252)/float(history['num_days'].max())))-1
			macroindicators=macroindicators.to_frame('data')
			macroindicators[' ']=macroindicators.index #add a column with no name, just so it looks pretty when we print it
			macroindicators=macroindicators[[' ','data']]
			googlesheetuploader.pgs_upload_df_to_worksheet(macroindicators,account['googlesheetid'],'macroindicators',service_file=self.key_file)
def main(config_file):
	p = Performance(config_file=config_file)
	p.update_performance_spreadsheets()
	return
if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	p=Performance(config_file='finance_cfg.cfg')
	p.update_performance_spreadsheets()