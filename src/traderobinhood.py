import robinhoodwrapper
import logging
import inspect
import pandas as pd
import commonqueries
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas_market_calendars as mcal
import pytz
import os
import configwrapper

class TradeRobinhood():
	def __init__(self,config_file):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		data_collections=self.build_collections('FINANCIALDATA_COLLECTIONS')
		user_collections=self.build_collections('USERS_COLLECTIONS')
		self.data_collections=data_collections
		self.user_collections=user_collections
		self.data_cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=data_collections)
		self.user_cq=commonqueries.CommonQueries(port=self.config.get_int('USERS_MONGO','port'),host=self.config.get_string('USERS_MONGO','host'), username=self.config.get_string('USERS_MONGO','username'), password=self.config.get_string('USERS_MONGO','password'), dbname=self.config.get_string('USERS_MONGO','dbname'),collections=user_collections)
		return
	def get_trade_now(self):
		x=mcal.get_calendar('NYSE').schedule(start_date=datetime.now().date()-relativedelta(days=7),end_date=datetime.now().date()+relativedelta(days=7))
		now = pytz.utc.localize(datetime.utcnow())
		today=now.date()
		x=x[pd.to_datetime(x['market_open'])>=now]
		time_until_market_open=float((x['market_open'].iloc[0]-now).total_seconds())
		max_time_between_close_and_open=float(17.5*60*60) #4:00pm until 9:30 the next day, is 7.5 hours
		tradenow=True
		if time_until_market_open>max_time_between_close_and_open:
			logging.info('more than 7.5 hours until the next market open, not trading now')
			tradenow=False
		return tradenow
	def build_collections(self,section='FINANCIALDATA_COLLECTIONS'):
		self.user_collections={}
		for option in self.config.get_options(section):
			self.user_collections[option]=self.config.get_string(section,option)
		return self.user_collections
	def trade_robinhood(self):


		recommended_portfolio=pd.DataFrame(list(self.data_cq.mongo.db[self.data_collections['quantative_value_recommended']].find({},{'_id':0})))
		#calculate aech companyies empercenftage.
		for row, company in recommended_portfolio.iterrows():
			empercentage=self.data_cq.get_percent_greater_than(self.data_collections['metrics'],self.data_cq.ticker2cik(company['ticker']),'emyield')
			recommended_portfolio.loc[row,'empercentage']=1-empercentage
		min_empercentage=float(recommended_portfolio['empercentage'].min()) #the value where we will sell any stock less than this number

		user_df = pd.DataFrame(list(self.user_cq.mongo.db[self.user_collections['robinhood_users']].find()))
		user_df = user_df.sort_values('username')
		user_df = user_df.drop_duplicates('username')  # has the usernames and passwords of all robinhood users

		rh_generic = robinhoodwrapper.RobinHoodWrapper(instruments=self.data_cq.get_robinhood_instruments())

		for row, data in recommended_portfolio.iterrows():
			recommended_portfolio.loc[row, 'robinhood_price'] = rh_generic.get_last_price(data['ticker'])
			recommended_portfolio.loc[row, 'instrument'] = rh_generic.symbol2instrument(data['ticker'])

		if (recommended_portfolio['price'] != recommended_portfolio['robinhood_price']).any():
			logging.error('pricemismatch')
			logging.error(str(recommended_portfolio[recommended_portfolio['price'] != recommended_portfolio['robinhood_price']]))
			recommended_portfolio.to_csv('recommended_portfolio.csv')
			if len(recommended_portfolio[recommended_portfolio['price'] != recommended_portfolio['robinhood_price']]) >= .1 * float(len(recommended_portfolio)):  # if more than 10% of the companies dont match
				logging.error('more than 10 percent of the companies dont match, dont trade, something is wrong')
				return
		recommended_portfolio=recommended_portfolio[pd.notnull(recommended_portfolio['price'])]
		recommended_portfolio=recommended_portfolio[pd.notnull(recommended_portfolio['robinhood_price'])]
		recommended_portfolio['price']=recommended_portfolio['price'].round(2)
		recommended_portfolio['robinhood_price']=recommended_portfolio['robinhood_price'].round(2)
		recommended_portfolio['weight']=recommended_portfolio['weight']/(recommended_portfolio['weight'].sum())
		recommended_portfolio=recommended_portfolio.set_index('ticker',drop=False)
		if len(recommended_portfolio)==0:
			logging.error('empty trade dataframe')
			return
		recommended_portfolio_orig = recommended_portfolio.copy(deep=True)
		for index,account in user_df.iterrows():
			rh_user=robinhoodwrapper.RobinHoodWrapper(username=account['username'],password=account['password'],instruments=self.data_cq.get_robinhood_instruments())

			#get all the options from the user
			user_trade_options=account['trade']
			should_trade_now=self.get_trade_now()
			live_trade=user_trade_options['live_trade']
			options_trade=user_trade_options['options_trade']
			can_trade_options=rh_user.can_trade_options()
			master_options_trade=self.config.get_bool('TRADING','trade_options')
			master_live_trade=self.config.get_bool('TRADING','live_trade')

			if master_options_trade is False or not can_trade_options or not options_trade:
				options_trade=False
			if not live_trade or not should_trade_now or master_live_trade is False:
				live_trade=False
			if float(rh_user.get_accounts()[0]['cash'])==0:
				logging.info('we have no money to trade today')
				continue

			#FIRST WE DO THE BUYS
			recommended_portfolio=recommended_portfolio_orig.copy(deep=True)

			#filter out wash sale symbols, this way we are always fully invested as we are able
			washsalesymboles=rh_user.get_wash_sale_symbols()
			recommended_portfolio=recommended_portfolio[~recommended_portfolio['ticker'].isin(washsalesymboles)]
			recommended_portfolio['weight']=recommended_portfolio['weight']/(recommended_portfolio['weight'].sum())
			current_positions=rh_user.get_positions()
			recommended_portfolio['desired_value']=recommended_portfolio['weight']*(float(rh_user.get_total_portfolio_value())+float(rh_user.get_accounts()[0]['cash']))
			current_positions=current_positions[current_positions['instrument'].isin(recommended_portfolio['instrument'])] #filter our current positions so we only look at positions we have that we also want to buy
			recommended_portfolio['current_value']=float(0)
			for index,row in current_positions.iterrows():
				recommended_portfolio.loc[rh_user.instrument2symbol(row['instrument']),'current_value']=float(row['quantity'])*float(row['last_trade_price'])

			#we need to see if we have any current put option positions and take this into account and modify the current_value
			if options_trade is True:
				current_options_positions=rh_user.get_options_positions()
				if current_options_positions is not None and len(current_options_positions)>0:
					#todo 6/28 we still need to adjust hte current value of positions with outstanding options, both call and put
					current_options_positions=current_options_positions[current_options_positions['type']=='put']
					if len(current_options_positions)>0:
						logging.error('we need to do something with the optoins we have in our account because we now actually have put options')
						current_options_positions.to_csv('current_options_positions.csv')
						exit()

			recommended_portfolio['new_value']=recommended_portfolio['desired_value']-recommended_portfolio['current_value']
			recommended_portfolio=recommended_portfolio[recommended_portfolio['new_value']>0] #we only take buys, we dont worry about that we are overallocated to
			recommended_portfolio['new_weight']=recommended_portfolio['new_value']/(recommended_portfolio['new_value'].sum())
			recommended_portfolio['today_value_add']=recommended_portfolio['new_weight']*float(rh_user.get_accounts()[0]['cash'])
			recommended_portfolio['shares']=recommended_portfolio['today_value_add']/(recommended_portfolio['price'])
			recommended_portfolio['max_shares']=np.floor(recommended_portfolio['new_value']/(recommended_portfolio['price'])) #the maximum number of shares we would want to purchase today
			recommended_portfolio=recommended_portfolio.sort_values('shares',ascending=False)

			while any(recommended_portfolio['shares']<1) and len(recommended_portfolio)>0:
				recommended_portfolio=recommended_portfolio[:-1]
				recommended_portfolio['new_weight']=recommended_portfolio['today_value_add']/(recommended_portfolio['today_value_add'].sum())
				recommended_portfolio['today_value_add']=recommended_portfolio['new_weight']*float(rh_user.get_accounts()[0]['cash'])
				recommended_portfolio['shares']=recommended_portfolio['today_value_add']/(recommended_portfolio['price']) #we will only purchase at this limit price
				recommended_portfolio=recommended_portfolio.sort_values('shares',ascending=False)
			if len(recommended_portfolio)==0:
				logging.info('empty recommended df after filtering for shares')
				continue
			recommended_portfolio['shares']=np.floor(recommended_portfolio['shares'])
			recommended_portfolio['shares']=recommended_portfolio[['shares','max_shares']].min(axis=1) #take the minimum of what we are going to by, and the max we should, this will ensure that we never overallocate

			if live_trade:
				rh_user.cancel_all_orders() #ONLY REMOVE THE open stock orders, we really should not NEED to cancel, we can work it into our calculations
				if options_trade is True:
					rh_user.cancel_all_options_orders() #removes all current option orders
			logging.info(recommended_portfolio)
			for symbol,order in recommended_portfolio.iterrows():
				if options_trade is True:
					if float(order['shares'])>100:
						option_chain=self.data_cq.convert_option_chain_rh2td(symbol=symbol,stock_price=rh_user.get_last_price(symbol),option_chain=rh_user.get_options_instrument_data(symbol=symbol))
						best_put_to_sell=self.data_cq.get_best_put_to_sell(symbol,option_chain=option_chain,exercise_fee=0,trading_fee=0,contract_fee=0)
						if pd.notnull(best_put_to_sell):
							logging.error('prehaps we want to sell a put option?')
							logging.error('we also need to change the robinhoodwrapper get_positions_by_odrers to incorporate options events...')
							exit()
						else:
							if live_trade:
								rh_user.submitt_order(symbol=symbol, quantity=order['shares'], price=float(order['price']))
					else:
						if live_trade:
							rh_user.submitt_order(symbol=symbol,quantity=order['shares'],price=float(order['price']))
				else:
					if live_trade:
						rh_user.submitt_order(symbol=symbol, quantity=order['shares'], price=float(order['price']))
			if options_trade is True:
				#see if we have to sell any calls, this will later go inside of the if statement, we need to also see if we have any calls already
				positions=rh_user.get_positions()
				positions['shares_to_sell']=positions['quantity'].astype('float')-positions['shares_held'].astype('float')
				positions=positions[positions['shares_to_sell']>=100]
				for row,position in positions.iterrows():
					option_chain=self.data_cq.convert_option_chain_rh2td(symbol=position['symbol'],stock_price=rh_user.get_last_price(position['symbol']),option_chain=rh_user.get_options_instrument_data(symbol=position['symbol']))
					positions.loc[row,'call_to_sell_symbol']=self.data_cq.get_best_call_to_sell(position['symbol'],option_chain=option_chain,exercise_fee=0,trading_fee=0,contract_fee=0)
					positions.loc[row,'num_calls_to_sell']=np.floor(float(position['shares_to_sell'])/100)
					positions.loc[row,'valid_trade']=self.data_cq.is_valid_trade(cik=self.data_cq.ticker2cik(position['symbol']))
					positions.loc[row, 'has_split']=self.data_cq.has_split(cik=self.data_cq.ticker2cik(position['symbol']))
				positions=positions[pd.notnull(positions['call_to_sell_symbol'])]
				positions=positions[positions['num_calls_to_sell']>0]
				positions = positions[positions['valid_trade'] == True]
				positions = positions[positions['has_split'] == False]

				positions=positions[['symbol','quantity','num_calls_to_sell','call_to_sell_symbol']]

				positions['num_calls_to_sell']=(positions['num_calls_to_sell'].astype('int'))*-1
				for index,position in positions.iterrows():
					option_chain=self.data_cq.convert_option_chain_rh2td(symbol=position['symbol'],stock_price=rh_user.get_last_price(position['symbol']),option_chain=rh_user.get_options_instrument_data(symbol=position['symbol']))
					call_option_to_sell=option_chain[option_chain['url']==position['call_to_sell_symbol']].iloc[0].to_dict()
					if live_trade:
						rh_user.submitt_option_order(position_effect="open",options_instrument_url_id=position['call_to_sell_symbol'],order_type="limit",quantity=position['num_calls_to_sell'],price=call_option_to_sell['bid_price'],account=None,time_in_force='gfd')
			else:
				pass

			#NOW WE DO THE SELLS
			sell_items=rh_user.get_sell_items()
			logging.info(sell_items)
			for symbol,value in sell_items.iteritems():
				last_price=rh_user.get_last_price(symbol=symbol)
				valid_trade=self.data_cq.is_valid_trade(cik=self.data_cq.ticker2cik(symbol))
				has_split=self.data_cq.has_split(cik=self.data_cq.ticker2cik(symbol))

				empercentage=self.data_cq.get_percent_greater_than(self.data_collections['metrics'],self.data_cq.ticker2cik(symbol),'emyield')
				if empercentage is None:
					empercentage=1 #if we can't find one, then we sell because assume that everyone is greater than us
				empercentage=1-empercentage

				next_release=self.data_cq.get_next_release(self.data_cq.ticker2cik(ticker=symbol))
				if next_release is None:
					next_release=datetime.now()+relativedelta(years=1) #this will ensure that we sell the stock
				days_until_release=self.data_cq.get_days_until_date(date=next_release)

				if empercentage<min_empercentage and days_until_release>1 and valid_trade and has_split is False:
					logging.info('we are selling stock:'+symbol+":"+str(value))
					if live_trade:
						rh_user.submitt_order(symbol=symbol, quantity=int(value), price=float(last_price))
			rh_user.logout() #we probably dont actually need to logout
		return
def main(config_file):
	t=TradeRobinhood(config_file=config_file)
	t.trade_robinhood()
	return
if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	main(config_file='finance_cfg.cfg')