import logging
import inspect
import mongomanager
import pandas_market_calendars as mcal
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
import commonqueries
import os
import configwrapper

class RecommendedPortfolios():
	def __init__(self,config_file):
		self.config = configwrapper.ConfigWrapper(config_file=config_file)
		collections=self.build_collections()
		self.collections=collections
		self.cq=commonqueries.CommonQueries(port=self.config.get_int('FINANCIALDATA_MONGO','port'),host=self.config.get_string('FINANCIALDATA_MONGO','host'), username=self.config.get_string('FINANCIALDATA_MONGO','username'), password=self.config.get_string('FINANCIALDATA_MONGO','password'), dbname=self.config.get_string('FINANCIALDATA_MONGO','dbname'),collections=collections)
		return
	def build_collections(self):
		collections={}
		for option in self.config.get_options('FINANCIALDATA_COLLECTIONS'):
			collections[option]=self.config.get_string('FINANCIALDATA_COLLECTIONS',option)
		return collections
	def get_quantative_value_recommended_portfolio(self):
		cq=self.cq
		collections=self.collections
		last_valid_day=pd.to_datetime(cq.get_last_complete_market_day())
		logging.info('last valid day:'+str(last_valid_day))

		qvdf=pd.DataFrame()
		for company in cq.mongo.db[collections['metrics']].find({}):
			qvdf=qvdf.append(company,ignore_index=True)
		if len(qvdf)==0:
			logging.error('empty recommendeddf')
			exit()


		#googlesheetuploader.uploaddftogooglesheets(qvdf,'quantvalue','raw_metrics',chunksize=100) #this will eventually go away
		#initial filter
		qvdf=qvdf[pd.to_datetime(qvdf['release_date']).dt.date<last_valid_day.date()]
		qvdf=qvdf[pd.to_datetime(qvdf['next_release']).dt.date>=datetime.today().date()+relativedelta(days=1)]
		qvdf=qvdf[pd.to_datetime(qvdf['end_date']).dt.date>=last_valid_day.date()-relativedelta(months=6)]
		qvdf=qvdf[pd.to_datetime(qvdf['lastpriceday']).dt.date>=last_valid_day.date()]
		qvdf=qvdf[qvdf['split_since_last_statement']==False]
		qvdf = qvdf[qvdf['valid_trade'] == True]
		#filter out companies that will complicate my taxes
		qvdf=qvdf[~qvdf['name'].str[-2:].str.contains('LP')]
		qvdf=qvdf[~qvdf['name'].str[-3:].str.contains('LLC')]

		#Filter out Financial and Utilities
		s=qvdf['industry_category'].isin([None,"Banking","Financial Services","Real Estate","Utilities"])
		qvdf=qvdf[~s]
		qvdf=qvdf[pd.notnull(qvdf['industry_category'])] #Make sure everything is real...

		#FILTER OUT MANIPULATORS OR DISTRESS COMPANIES
		#drop any companyes where either sta or snoa is nan, we only want to keep companies we can actually measure financial distress
		qvdf = qvdf[((pd.notnull(qvdf['sta']))|(pd.notnull(qvdf['snoa'])))&(pd.notnull(qvdf['pman']))&(pd.notnull(qvdf['pfd']))] #make sure one or the other is not nan
		qvdf = qvdf[(pd.notnull(qvdf['roa']))&(pd.notnull(qvdf['roc']))&(pd.notnull(qvdf['cfoa']))&((pd.notnull(qvdf['mg']))|(pd.notnull(qvdf['ms'])))]

		if len(qvdf)==0:
			logging.error('empty qvdf')
			exit()
		qvdf=qvdf.sort_values(['sta'],na_position='last')#the lower the better
		totallen=len(qvdf[pd.notnull(qvdf['sta'])])
		i=1
		for index,row in qvdf[pd.notnull(qvdf['sta'])].iterrows():
			qvdf.loc[index,"p_sta"]=float(i)/float(totallen)
			i+=1

		qvdf=qvdf.sort_values(['snoa'],na_position='last') #the lower the better
		totallen=len(qvdf[pd.notnull(qvdf['snoa'])])
		i=1
		for index,row in qvdf[pd.notnull(qvdf['snoa'])].iterrows():
			qvdf.loc[index,"p_snoa"]=float(i)/float(totallen)
			i+=1

		qvdf['comboaccrual']=qvdf[["p_snoa","p_sta"]].mean(axis=1)
		qvdf=qvdf[pd.notnull(qvdf['comboaccrual'])]

		qvdf=qvdf.sort_values(['pman'],na_position='last')#the lower the better
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_pman"]=float(i)/float(len(qvdf))
			i+=1

		qvdf=qvdf.sort_values(['pfd'],na_position='last')#the lower the better
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_pfd"]=float(i)/float(len(qvdf))
			i+=1

		cutoff=.95
		s=(qvdf['comboaccrual']<cutoff)&(qvdf['p_pman']<cutoff)&(qvdf['p_pfd']<cutoff)
		qvdf=qvdf[s]

		qvdf = qvdf[(pd.notnull(qvdf['roa']))&(pd.notnull(qvdf['roc']))&(pd.notnull(qvdf['cfoa']))&((pd.notnull(qvdf['mg']))|(pd.notnull(qvdf['ms'])))]

		qvdf=qvdf.sort_values(['roa'],na_position='first') #the higher
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_roa"]=float(i)/float(len(qvdf))
			i+=1

		qvdf=qvdf.sort_values(['roc'],na_position='first') #the higher
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_roc"]=float(i)/float(len(qvdf))
			i+=1

		qvdf=qvdf.sort_values(['cfoa'],na_position='first') #the higher
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_cfoa"]=float(i)/float(len(qvdf))
			i+=1

		qvdf=qvdf.sort_values(['mg'],na_position='first') #the higher
		i=1
		totallen=len(qvdf[pd.notnull(qvdf['mg'])])
		for index,row in qvdf[pd.notnull(qvdf['mg'])].iterrows():
			qvdf.loc[index,"p_mg"]=float(i)/float(totallen)
			i+=1

		qvdf=qvdf.sort_values(['ms'],na_position='first') #the higher
		i=1
		totallen=len(qvdf[pd.notnull(qvdf['ms'])])
		for index,row in qvdf[pd.notnull(qvdf['ms'])].iterrows():
			qvdf.loc[index,"p_ms"]=float(i)/float(totallen)
			i+=1

		qvdf['marginmax']=qvdf[["p_ms","p_mg"]].max(axis=1)

		qvdf['franchisepower']=qvdf[["marginmax","p_roa","p_roc","p_cfoa"]].mean(axis=1)

		qvdf=qvdf[pd.notnull(qvdf['franchisepower'])]
		qvdf=qvdf.sort_values(['franchisepower'],na_position='first') #the higher
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_fp"]=float(i)/float(len(qvdf))
			i+=1
		qvdf['quality']=.5*qvdf["p_fp"]+.5*qvdf["fs"] #Final quality measure

		qvdf=qvdf[pd.notnull(qvdf['emyield'])]
		qvdf=qvdf.sort_values(['emyield'],na_position='first') #the higher
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_emyield"]=float(i)/float(len(qvdf))
			i+=1

		s=qvdf['p_emyield']>=.9
		qvdf=qvdf[s]

		qvdf=qvdf.sort_values(['quality'],na_position='first')
		i=1
		for index,row in qvdf.iterrows():
			qvdf.loc[index,"p_quality"]=float(i)/float(len(qvdf))
			i+=1

		s=qvdf['p_quality']>=.5
		qvdf=qvdf[s]

		badrows=(qvdf['newshares']>0)&(qvdf['sec13']==0)&(qvdf['daystocover']>1)
		goodrows=(qvdf['newshares']<=0)|(qvdf['sec13']>0)|(qvdf['daystocover']<=1)|(qvdf['insider_purchase_ratio'].astype('float')>0)
		#qvdf=qvdf[~badrows]
		qvdf=qvdf[goodrows]

		# qvdf=qvdf.sort_values('fip')
		# qvdf=qvdf[:30] #only select the top 30 companies for the portfolio
		qvdf['weight']=float(1)/float(len(qvdf))

		qvdf=qvdf[['ticker','name','industry_group','emyield','price','marketcap','weight']]
		qvdf=qvdf.set_index('ticker')

		cq.mongo.db[collections['quantative_value_recommended']].remove({})

		#googlesheetuploader.uploaddftogooglesheets(qvdf,'quantvalue','quantative_value_recommended') #this will eventually go away
		for index,row in qvdf.iterrows():
			data=row.to_dict()
			data['ticker']=index
			cq.mongo.db[collections['quantative_value_recommended']].insert(data)
		return qvdf

def main(config_file=None):
	r=RecommendedPortfolios(config_file=config_file)
	r.get_quantative_value_recommended_portfolio()
if __name__ == '__main__':
	if os.path.exists(inspect.stack()[0][1].replace('py','log')):
		os.remove(inspect.stack()[0][1].replace('py','log'))
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	pass