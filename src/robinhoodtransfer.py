
import robinhoodwrapper
import logging
import inspect
import commonqueries
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas_market_calendars as mcal
import pytz
import configwrapper

class RobinhoodTransfer():
	def __init__(self,config_file):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		data_collections=self.build_collections('FINANCIALDATA_COLLECTIONS')
		user_collections=self.build_collections('USERS_COLLECTIONS')
		self.data_collections=data_collections
		self.user_collections=user_collections
		self.data_cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=data_collections)
		self.user_cq=commonqueries.CommonQueries(port=self.config.get_int('USERS_MONGO','port'),host=self.config.get_string('USERS_MONGO','host'), username=self.config.get_string('USERS_MONGO','username'), password=self.config.get_string('USERS_MONGO','password'), dbname=self.config.get_string('USERS_MONGO','dbname'),collections=user_collections)
		return
	def build_collections(self,section='FINANCIALDATA_COLLECTIONS'):
		collections={}
		for option in self.config.get_options(section):
			collections[option]=self.config.get_string(section,option)
		return collections
	def transfer_funds_to_robinhood(self):
		x=mcal.get_calendar('NYSE').schedule(start_date=datetime.now().date()-relativedelta(days=7),end_date=datetime.now().date()+relativedelta(days=7))
		now = pytz.utc.localize(datetime.utcnow())
		today=now.date()
		x=x[pd.to_datetime(x['market_open'])>=now]
		time_until_market_open=float((x['market_open'].iloc[0]-now).total_seconds())
		max_time_between_close_and_open=float(17.5*60*60) #4:00pm until 9:30 the next day, is 7.5 hours
		if time_until_market_open>max_time_between_close_and_open:
			logging.info('more than 7.5 hours until the next market open, not trading now')
			return
		cq=self.user_cq
		user_df=pd.DataFrame(list(cq.mongo.db[self.user_collections['robinhood_users']].find()))
		user_df=user_df.sort_values('username')
		user_df=user_df.drop_duplicates('username') #has the usernames and passwords of all robinhood users

		for index,account in user_df.iterrows():
			if 'transfer' not in account or pd.isnull(account['transfer']) or len(account['transfer'])==0:
				continue
			rh_user=robinhoodwrapper.RobinHoodWrapper(username=account['username'],password=account['password'])
			transferinfo=account['transfer']
			id=transferinfo['id']
			frequency=transferinfo['frequency']
			amount=transferinfo['amount']
			if 'last_transfer_id' not in transferinfo or pd.isnull(transferinfo['last_transfer_id']):
				rh_user.bank2rh(amount,id)
				transfers=rh_user.get_ach_transfers()
				transfers=transfers[transfers['direction']=='deposit']
				transfers['created_at']=pd.to_datetime(transfers['created_at'])
				transfers=transfers.sort_values('created_at',ascending=True)
				id=transfers['id'].iloc[-1]
				transferinfo['last_transfer_id']=id
				cq.mongo.db[self.user_collections['robinhood_users']].update({"_id":account["_id"]},account.to_dict())
			else:
				now=pd.to_datetime(datetime.utcnow())
				transfers=rh_user.get_ach_transfers()
				lasttransfertime=pd.to_datetime(transfers[transfers['id']==transferinfo['last_transfer_id']]['created_at'].iloc[0])
				frequency_multiple=transferinfo['frequency_multiple']
				if frequency=='daily':
					delta=relativedelta(days=1)*frequency_multiple
				elif frequency=='weekly':
					delta=relativedelta(weeks=1)*frequency_multiple
				elif frequency=='monthly':
					delta=relativedelta(months=1)*frequency_multiple
				elif frequency=='yearly':
					delta=relativedelta(years=1)*frequency_multiple
				else:
					logging.error('unknown frequency')
					exit()
				if lasttransfertime.date()<=(now.date()-delta):
					rh_user.bank2rh(amount,id)
					transfers=rh_user.get_ach_transfers()
					transfers=transfers[transfers['direction']=='deposit']
					transfers['created_at']=pd.to_datetime(transfers['created_at'])
					transfers=transfers.sort_values('created_at',ascending=True)
					id=transfers['id'].iloc[-1]
					transferinfo['last_transfer_id']=id
					cq.mongo.db[self.user_collections['robinhood_users']].update({"_id":account["_id"]},account.to_dict())
				else:
					logging.info('too soon to transfer for account:'+str(account['_id']))
			rh_user.logout()
def main(config_file):
	r=RobinhoodTransfer(config_file=config_file)
	r.transfer_funds_to_robinhood()
	return
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')

	

