#code will get the proper values like emyield, marketcap, cacl, etc, and supply a string and value to put back into the dataframe.
import pandas as pd
import numpy as np
import logging
import inspect
from scipy import stats
from dateutil.relativedelta import relativedelta
from datetime import datetime
from scipy import stats
import math

class quantvaluedata:	#just contains functions, will NEVEFR actually get the data	
	def __init__(self,allitems=None):
		if allitems is None:
			self.allitems=[]
		else:
			self.allitems=allitems
		return
	def get_value(self,origdf,key,i=-1):
		if key not in origdf.columns and key not in self.allitems and key not in ['timedepositsplaced','fedfundssold','interestbearingdepositsatotherbanks']:
			logging.error(key+' not found in allitems')
			#logging.error(self.allitems)
			return None
		df=origdf.copy()
		df=df.sort_values('yearquarter')
		if len(df)==0:
			##logging.error("empty dataframe")
			return None
		if key not in df.columns:
			#logging.error("column not found:"+key)
			return None
		interested_quarter=df['yearquarter'].iloc[-1]+i+1#because if we want the last quarter we need them equal
		if not df['yearquarter'].isin([interested_quarter]).any(): #if the quarter we are interested in is not there
			return None		
		s=df['yearquarter']==interested_quarter
		df=df[s]
		if len(df)>1:
			logging.error(df)
			logging.error("to many rows in df")
			exit()
			pass
		value=df[key].iloc[0]
		if pd.isnull(value):
			return None
		return float(value)
	def get_sum_quarters(self,df,key,seed,length):
		values=[]
		#BIG BUG, this was origionally -length-1, which was always truncating the array and producing nans.
		periods=range(seed,seed-length,-1)
		for p in periods:
			values.append(self.get_value(df,key,p))
		#logging.info('values:'+str(values))
		if pd.isnull(values).any(): #return None if any of the values are None
			return None
		else:
			return float(np.sum(values))
	def get_market_cap(self,statements_df,prices_df,seed=-1):
		total_shares=self.get_value(statements_df,'weightedavedilutedsharesos',seed)
		if pd.isnull(total_shares):
			return None
		end_date=statements_df['end_date'].iloc[seed]
		if seed==-1: #get the latest price but see if there was a split between the end date and now
			s=pd.to_datetime(prices_df['date'])>pd.to_datetime(end_date)
			tempfd=prices_df[s]
			splits=tempfd['split_ratio'].unique()
			adj=pd.Series(splits).product() #multiply all the splits together to get the total adjustment factor from the last total_shares
			total_shares=total_shares*adj
			last_price=prices_df.sort_values('date').iloc[-1]['close']
			price=float(last_price)
			market_cap=price*float(total_shares)
			return market_cap
		else:
			marketcap=self.get_value(statements_df,'marketcap',seed)
			if pd.isnull(marketcap):
				return None
			else:
				return marketcap
	def get_netdebt(self,statements_df,seed=-1):
		shorttermdebt=self.get_value(statements_df,'shorttermdebt',seed)
		longtermdebt=self.get_value(statements_df,'longtermdebt',seed)
		capitalleaseobligations=self.get_value(statements_df,'capitalleaseobligations',seed)
		cashandequivalents=self.get_value(statements_df,'cashandequivalents',seed)
		restrictedcash=self.get_value(statements_df,'restrictedcash',seed)
		fedfundssold=self.get_value(statements_df,'fedfundssold',seed)
		interestbearingdepositsatotherbanks=self.get_value(statements_df,'interestbearingdepositsatotherbanks',seed)
		timedepositsplaced=self.get_value(statements_df,'timedepositsplaced',seed)	
		s=pd.Series([shorttermdebt,longtermdebt,capitalleaseobligations,cashandequivalents,restrictedcash,fedfundssold,interestbearingdepositsatotherbanks,timedepositsplaced]).astype('float')
		if pd.isnull(s).all(): #return None if everything is null
			return None
		m=pd.Series([1,1,1,-1,-1,-1,-1])
		netdebt=s.multiply(m).sum()
		return float(netdebt)
	def get_enterprise_value(self,statements_df,prices_df,seed=-1):
		#calculation taken from https://intrinio.com/data-tag/enterprisevalue
		marketcap=self.get_market_cap(statements_df,prices_df,seed)
		netdebt=self.get_netdebt(statements_df,seed)
		totalpreferredequity=self.get_value(statements_df,'totalpreferredequity',seed)
		noncontrollinginterests=self.get_value(statements_df,'noncontrollinginterests',seed)
		redeemablenoncontrollinginterest=self.get_value(statements_df,'redeemablenoncontrollinginterest',seed)
		s=pd.Series([marketcap,netdebt,totalpreferredequity,noncontrollinginterests,redeemablenoncontrollinginterest])
		if pd.isnull(s).all() or pd.isnull(marketcap):
			return None
		return float(s.sum())		
		
	def get_ebit(self,df,seed=-1,length=4):
		ebit=self.get_sum_quarters(df,'totaloperatingincome',seed,length)
		if pd.notnull(ebit):
			return float(ebit)
		totalrevenue=self.get_sum_quarters(df,'totalrevenue',seed,length)
		provisionforcreditlosses=self.get_sum_quarters(df,'provisionforcreditlosses',seed,length)
		totaloperatingexpenses=self.get_sum_quarters(df,'totaloperatingexpenses',seed,length)
		s=pd.Series([totalrevenue,provisionforcreditlosses,totaloperatingexpenses])
		if pd.isnull(s).all():
			return None
		ebit=(s.multiply(pd.Series([1,-1,-1]))).sum()
		if pd.notnull(ebit):
			return float(ebit)
		return None
		
	def get_emyield(self,statements_df,prices_df,seed=-1,length=4):
		ebit=self.get_ebit(statements_df,seed,length)
		enterprisevalue=self.get_enterprise_value(statements_df,prices_df,seed)
		if pd.isnull([ebit,enterprisevalue]).any() or enterprisevalue==0:
			return None
		return float(ebit/enterprisevalue)		
	def get_scalednetoperatingassets(self,statements_df,seed=-1):
		"""
		SNOA = (Operating Assets  Operating Liabilities) / Total Assets
		where
		OA = total assets  cash and equivalents
		OL = total assets  ST debt  LT debt  minority interest - preferred stock - book common
		
		oa=ttmsdfcompany.iloc[-1]['totalassets']-ttmsdfcompany.iloc[-1]['cashandequivalents']
		ol=ttmsdfcompany.iloc[-1]['totalassets']-ttmsdfcompany.iloc[-1]['netdebt']-ttmsdfcompany.iloc[-1]['totalequityandnoncontrollinginterests']
		snoa=(oa-ol)/ttmsdfcompany.iloc[-1]['totalassets']
		"""
		totalassets=self.get_value(statements_df,'totalassets',seed)
		cashandequivalents=self.get_value(statements_df,'cashandequivalents',seed)
		netdebt=self.get_netdebt(statements_df,seed)
		totalequityandnoncontrollinginterests=self.get_value(statements_df,'totalequityandnoncontrollinginterests',seed)
		
		if pd.isnull(totalassets) or totalassets==0:
			return None
		
		s=pd.Series([totalassets,cashandequivalents])
		m=pd.Series([1,-1])
		oa=s.multiply(m).sum()
		s=pd.Series([totalassets,netdebt,totalequityandnoncontrollinginterests])
		m=pd.Series([1,-1,-1])
		ol=s.multiply(m).sum()
		scalednetoperatingassets=(oa-ol)/totalassets
		return float(scalednetoperatingassets)
	def get_scaledtotalaccruals(self,statements_df,seed=-1,length=4):
		netincome=self.get_sum_quarters(statements_df,'netincome',seed,length)
		netcashfromoperatingactivities=self.get_sum_quarters(statements_df,'netcashfromoperatingactivities',seed,length)
		start_assets=self.get_value(statements_df,'cashandequivalents',seed-length)
		end_assets=self.get_value(statements_df,'cashandequivalents',seed)
		if pd.isnull([start_assets,end_assets]).any():
			return None
		totalassets=np.mean([start_assets,end_assets])
		if pd.isnull(totalassets):
			return None
		num=pd.Series([netincome,netcashfromoperatingactivities])
		if pd.isnull(num).all():
			return None
		m=pd.Series([1,-1])
		num=num.multiply(m).sum()
		den=totalassets
		if den==0:
			return None
		scaledtotalaccruals=num/den
		return float(scaledtotalaccruals)
	def get_grossmargin(self,statements_df,seed=-1,length=4):
		totalrevenue=self.get_sum_quarters(statements_df, 'totalrevenue', seed, length)
		totalcostofrevenue=self.get_sum_quarters(statements_df, 'totalcostofrevenue', seed, length)
		if pd.isnull([totalrevenue,totalcostofrevenue]).any() or totalcostofrevenue==0:
			return None
		grossmargin=(totalrevenue-totalcostofrevenue)/totalcostofrevenue
		return float(grossmargin)
	def get_margingrowth(self,statements_df,seed=-1,length1=20,length2=4):
		grossmargins=[]
		for i in range(seed,seed-length1,-1):
			grossmargins.append(self.get_grossmargin(statements_df, i, length2))
		grossmargins=pd.Series(grossmargins)
		if pd.isnull(grossmargins).any():
			return None
		growth=grossmargins.pct_change(periods=1)
		growth=growth[pd.notnull(growth)]
		if len(growth)==0:
			return None
		grossmargingrowth=stats.gmean(1+growth)-1
		if pd.isnull(grossmargingrowth):
			return None
		return float(grossmargingrowth)		
	def get_marginstability(self,statements_df,seed=-1,length1=20,length2=4):
		#length1=how far back to go, how many quarters to get 20 quarters
		#length2=for each quarter, how far back to go 4 quarters
		grossmargins=[]
		for i in range(seed,seed-length1,-1):
			grossmargins.append(self.get_grossmargin(statements_df, i, length2))
		grossmargins=pd.Series(grossmargins)
		if pd.isnull(grossmargins).any() or grossmargins.std()==0:
			return None
		marginstability=grossmargins.mean()/grossmargins.std()
		if pd.isnull(marginstability):
			return None         
		return float(marginstability)
	def get_cacl(self,df,seed=-1):
		a=self.get_value(df,'totalcurrentassets',seed)
		l=self.get_value(df,'totalcurrentliabilities',seed)
		if pd.isnull([a,l]).any() or l==0:
			return None
		else:
			return a/l
	def get_tatl(self,df,seed=-1):
		a=self.get_value(df,'totalassets',seed)
		l=self.get_value(df,'totalliabilities',seed)
		if pd.isnull([a,l]).any() or l==0:
			return None
		else:
			return a/l
	def get_longterm_cacl(self,df,seed=-1,length=20):
		ltcacls=[]
		for i in range(seed,seed-length,-1):
			ltcacls.append(self.get_cacl(df,i))
		ltcacls=pd.Series(ltcacls)
		if pd.isnull(ltcacls).any():
			return None
		return stats.gmean(1+ltcacls)-1 #not totally sure we need the 1+, and the -1 11/9/17
	def get_longterm_tatl(self,df,seed=-1,length=20):
		lttatls=[]
		for i in range(seed,seed-length,-1):
			lttatls.append(self.get_tatl(df,i))
		lttatls=pd.Series(lttatls)
		if pd.isnull(lttatls).any():
			return None
		return stats.gmean(1+lttatls)-1 #not totally sure we need the 1+, and the -1 11/9/17
	def get_capex(self,df,seed=-1,length=4):
		purchaseofplantpropertyandequipment=self.get_sum_quarters(df,'purchaseofplantpropertyandequipment',seed,length)
		saleofplantpropertyandequipment=self.get_sum_quarters(df,'saleofplantpropertyandequipment',seed,length)
		s=pd.Series([purchaseofplantpropertyandequipment,saleofplantpropertyandequipment])
		if pd.isnull(s).all():
			return None
		m=pd.Series([-1,-1])
		capex=(s*m).sum()
		if capex is None:
			return None
		return float(capex)
	def get_freecashflow(self,df,seed=-1):
		netcashfromoperatingactivities=self.get_value(df,'netcashfromoperatingactivities',seed)
		capex=self.get_capex(df,seed,length=1)
		s=pd.Series([netcashfromoperatingactivities,capex])
		if pd.isnull(s).all():
			return None
		m=pd.Series([1,-1])
		fcf=(s*m).sum()
		return float(fcf)
	#add a length2 paramater so we take the sums of cash flows
	def get_cashflowonassets(self,df,seed=-1,length1=20,length2=4):
		cfoas=[]
		for i in range(seed,seed-length1,-1):
			start_assets=self.get_value(df,'totalassets',i-length2)
			end_assets=self.get_value(df,'totalassets',i)
			fcfs=[]
			for k in range(i,i-length2,-1):
				fcf=self.get_freecashflow(df,k)
				fcfs.append(fcf)
			if pd.isnull(fcfs).any():
				return None
			total_fcf=pd.Series(fcfs).sum()
			avg_assets=pd.Series([start_assets,end_assets]).mean()			
			if pd.isnull([total_fcf,avg_assets]).any() or avg_assets==0:
				return None
			else:
				cfoas.append(total_fcf/avg_assets)
		
		if pd.isnull(cfoas).any():
			return None
		else:
			if pd.isnull(stats.gmean(1+pd.Series(cfoas))-1):
				return None
			else:
				return stats.gmean(1+pd.Series(cfoas))-1 #we want to punish variability because the higher number the better
	def get_roa(self,df,seed=-1,length=4):
		netincome=self.get_sum_quarters(df,'netincome',seed,length)
		start_assets=self.get_value(df,'totalassets',seed-length)
		end_assets=self.get_value(df,'totalassets',seed)
		if pd.isnull([start_assets,end_assets]).any():
			return None
		totalassets=pd.Series([start_assets,end_assets]).mean()
		if pd.isnull([netincome,totalassets]).any() or totalassets==0:
			return None
		roa=netincome/totalassets
		return float(roa)
	def get_roc(self,df,seed=-1,length=4):
		ebit=self.get_ebit(df,seed,length)
		dividends=self.get_sum_quarters(df,'paymentofdividends',seed,length)

		start_debt=self.get_netdebt(df,seed-length)
		end_debt=self.get_netdebt(df,seed)
		netdebt=pd.Series([start_debt,end_debt]).mean()
		
		start_equity=self.get_value(df,'totalequity',seed-length)
		end_equity=self.get_value(df,'totalequity',seed)
		totalequity=pd.Series([start_equity,end_equity]).mean()
		num=pd.Series([ebit,dividends]).sum()
		den=pd.Series([netdebt,totalequity]).sum()
		if pd.isnull([num,den]).any() or den==0:
			return None
		else:
			roc=(float(num/den))        
		return float(roc)  
	def get_longtermroa(self,df,seed=-1,length1=20,length2=4):
		roas=[]
		for i in range(seed,seed-length1,-1):
			roas.append(self.get_roa(df,i,length2))
		if pd.isnull(roas).any():
			return None
		longtermroagmean=stats.gmean(1+pd.Series(roas))-1
		if pd.isnull(longtermroagmean):
			return None
		return float(longtermroagmean)
	def get_longtermroc(self,df,seed=-1,length1=20,length2=4):
		rocs=[]
		for i in range(seed,seed-length1,-1):
			rocs.append(self.get_roc(df,i,length2))
		rocs=pd.Series(rocs)
		if pd.isnull(rocs).any():
			return None
		roc=stats.gmean(1+rocs)-1
		if pd.isnull(roc):
			return None
		return float(roc)
	def get_momentum(self,df,period=relativedelta(months=11)):
		df=df[pd.to_datetime(df['date'])>=pd.to_datetime(df['date'].max())-period]
		df=df['adj_close'].astype('float')
		pctchange=df.pct_change()
		pctchange=pctchange.dropna()
		pctchange=1+pctchange
		pctchange=pctchange.tolist()
		gain=np.prod(pctchange)
		return float(gain-1)
	def get_fip(self,df,period=relativedelta(years=1)):
		orig_df=df.copy()
		df=df[pd.to_datetime(df['date'])>=pd.to_datetime(df['date'].max())-period]
		df=df['adj_close'].astype('float')
		pctchange=df.pct_change()
		pctchange=pctchange.dropna()
		if len(pctchange)==0:
			return None
		updays=(pctchange>0).sum()
		downdays=(pctchange<0).sum()
		fip=float(downdays)/float(len(pctchange))-float(updays)/float(len(pctchange))
		if self.get_momentum(orig_df)<0:
			fip=-1*fip
		return fip #the lower the better
		
	def get_balance_sheet_mean_value(self,df,tag,seed=-1,length=1):
		start=self.get_value(df,tag,seed-length)
		end=self.get_value(df,tag,seed)
		if pd.isnull(pd.Series([start,end])).any() or start==0 or end==0:
			return None
		average=pd.Series([start,end]).mean()
		if pd.isnull(average):
			return None
		else:
			return float(average)
		
	def get_dsri(self,df,seed1=-1,seed2=-5,length=4):
		#seed1 and 2 are the quarters we are comparing between
		#dsri=(ttmsdfcompany.iloc[-1]['accountsreceivable']/ttmsdfcompany.iloc[-1]['totalrevenue'])/(ttmsdfcompany.iloc[-5]['accountsreceivable']/ttmsdfcompany.iloc[-5]['totalrevenue'])
		#accountsreceivable1=self.get_value(cik,'balance_sheet','accountsreceivable',seed1)
		#accountsreceivable2=self.get_value(cik,'balance_sheet','accountsreceivable',seed2)
		accountsreceivable1=self.get_balance_sheet_mean_value(df, 'accountsreceivable', seed1,length)
		accountsreceivable2=self.get_balance_sheet_mean_value(df, 'accountsreceivable', seed2,length)
		
		totalrevenue1=self.get_sum_quarters(df,'totalrevenue',seed1,length)
		totalrevenue2=self.get_sum_quarters(df,'totalrevenue',seed2,length)
		
		if pd.isnull([accountsreceivable1,accountsreceivable2,totalrevenue1,totalrevenue2]).any() or totalrevenue1==0 or totalrevenue2==0:
			return None
		num=accountsreceivable1/totalrevenue1
		den=accountsreceivable2/totalrevenue2
		if den==0:
			return None
		dsri=num/den
		return float(dsri)
	def get_gmi(self,df,seed1=-1,seed2=-5,length=4):
		#gmi=((ttmsdfcompany.iloc[-5]['totalrevenue']-ttmsdfcompany.iloc[-5]['totalcostofrevenue'])/ttmsdfcompany.iloc[-5]['totalrevenue'])/((ttmsdfcompany.iloc[-1]['totalrevenue']-ttmsdfcompany.iloc[-1]['totalcostofrevenue'])/ttmsdfcompany.iloc[-1]['totalrevenue'])
		totalrevenue1=self.get_sum_quarters(df,'totalrevenue',seed1,length)
		totalrevenue2=self.get_sum_quarters(df,'totalrevenue',seed2,length)
		totalcostofrevenue1=self.get_sum_quarters(df,'totalcostofrevenue',seed1,length)
		totalcostofrevenue2=self.get_sum_quarters(df,'totalcostofrevenue',seed2,length)
		if pd.isnull([totalrevenue1,totalrevenue2,totalcostofrevenue1,totalcostofrevenue2]).any():
			return None
		if totalrevenue2==0 or totalrevenue1==0:
			return None
		num=(totalrevenue2-totalcostofrevenue2)/totalrevenue2
		den=(totalrevenue1-totalcostofrevenue1)/totalrevenue1
		gmi=num/den
		if den==0:
			return None
		return float(gmi)
	def get_aqi(self,df,seed1=-1,seed2=-5):
		#https://www.oldschoolvalue.com/blog/investment-tools/beneish-earnings-manipulation-m-score/
		#otherlta1=companydf.iloc[-1]['totalassets']-(companydf.iloc[-1]['totalcurrentassets']+companydf.iloc[-1]['netppe'])
		#otherlta2=companydf.iloc[-5]['totalassets']-(companydf.iloc[-5]['totalcurrentassets']+companydf.iloc[-5]['netppe'])
		# aqi=(otherlta1/companydf.iloc[-1]['totalassets'])/(otherlta2/companydf.iloc[-5]['totalassets'])
		netppe1=self.get_value(df,'netppe',seed1)
		netppe2=self.get_value(df,'netppe',seed2)
		totalassets1=self.get_value(df,'totalassets',seed1)
		totalassets2=self.get_value(df,'totalassets',seed2)
		totalcurrentassets1=self.get_value(df,'totalcurrentassets',seed1)
		totalcurrentassets2=self.get_value(df,'totalcurrentassets',seed2)
		
		if pd.isnull([netppe1,netppe2,totalassets1,totalassets2,totalcurrentassets1,totalcurrentassets2]).any():
			return None
		
		a=totalassets1-totalcurrentassets1-netppe1
		b=totalassets2-totalcurrentassets2-netppe2
		if totalassets1==0 or totalassets2==0:
			return None
		num=a/totalassets1
		den=b/totalassets2
		if den==0:
			return None
		aqi=num/den
		return float(aqi)
	def get_sgi(self,df,seed1=-1,seed2=-5,length=4):
		#sgi=ttmsdfcompany.iloc[-1]['totalrevenue']/ttmsdfcompany.iloc[-5]['totalrevenue']
		totalrevenue1=self.get_sum_quarters(df,'totalrevenue',seed1,length)
		totalrevenue2=self.get_sum_quarters(df,'totalrevenue',seed2,length)
		if pd.isnull([totalrevenue1,totalrevenue2]).any():
			return None
		if totalrevenue2==0:
			return None
		sgi=totalrevenue1/totalrevenue2
		return float(sgi)    
	def get_depi(self,df,seed1=-1,seed2=-5,length=4):
		#depit=ttmsdfcompany.iloc[-1]['depreciationexpense']/(ttmsdfcompany.iloc[-1]['depreciationexpense']+ttmsdfcompany.iloc[-1]['netppe'])
		#depit1=ttmsdfcompany.iloc[-5]['depreciationexpense']/(ttmsdfcompany.iloc[-5]['depreciationexpense']+ttmsdfcompany.iloc[-5]['netppe'])
		#depi=depit1/depit
		depreciationexpense1=self.get_sum_quarters(df,'depreciationexpense',seed1,length)
		depreciationexpense2=self.get_sum_quarters(df,'depreciationexpense',seed2,length)
		netppe1=self.get_balance_sheet_mean_value(df, 'netppe', seed1,length)
		netppe2=self.get_balance_sheet_mean_value(df, 'netppe', seed2,length)
		if pd.isnull([depreciationexpense1,depreciationexpense2,netppe1,netppe2]).any():
			return None
		num=depreciationexpense2/(depreciationexpense2+netppe2)
		den=depreciationexpense1/(depreciationexpense1+netppe1)
		if den==0:
			return None
		depi=num/den
		return float(depi)
	def get_sgai(self,df,seed1=-1,seed2=-5,length=4):
		#sgait=ttmsdfcompany.iloc[-1]['sgaexpense']/ttmsdfcompany.iloc[-1]['totalrevenue']
		#sgait1=ttmsdfcompany.iloc[-5]['sgaexpense']/ttmsdfcompany.iloc[-5]['totalrevenue']
		#sgai=sgait/sgait1        
		sgaexpense1=self.get_sum_quarters(df,'sgaexpense',seed1,length)
		sgaexpense2=self.get_sum_quarters(df,'sgaexpense',seed2,length)
		totalrevenue1=self.get_sum_quarters(df,'totalrevenue',seed1,length)
		totalrevenue2=self.get_sum_quarters(df,'totalrevenue',seed2,length)
		if pd.isnull([sgaexpense1,sgaexpense2,totalrevenue1,totalrevenue2]).any():
			return None
		if totalrevenue1==0 or totalrevenue2==0:
			return None
		num=sgaexpense1/totalrevenue1
		den=sgaexpense2/totalrevenue2
		if den==0:
			return None
		sgai=num/den
		return float(sgai)
	def get_lvgi(self,df,seed1=-1,seed2=-5):
		"""
		lvgit=(companydf.iloc[-1]['longtermdebt']+companydf.iloc[-1]['totalcurrentliabilities'])/companydf.iloc[-1]['totalassets']
		lvgit1=(companydf.iloc[-5]['longtermdebt']+companydf.iloc[-5]['totalcurrentliabilities'])/companydf.iloc[-5]['totalassets']
		lvgi=lvgit/lvgit1
		
		"""
		longtermdebt1=self.get_value(df,'longtermdebt',seed1)
		longtermdebt2=self.get_value(df,'longtermdebt',seed2)
		shorttermdebt1=self.get_value(df,'shorttermdebt',seed1)
		shorttermdebt2=self.get_value(df,'shorttermdebt',seed2)
		totalassets1=self.get_value(df,'totalassets',seed1)
		totalassets2=self.get_value(df,'totalassets',seed2)
		
		if pd.isnull([longtermdebt1,longtermdebt2,shorttermdebt1,shorttermdebt2,totalassets1,totalassets2]).any() or totalassets1==0 or totalassets2==0:
			return None
		num=(longtermdebt1+shorttermdebt1)/totalassets1
		den=(longtermdebt2+shorttermdebt2)/totalassets2
		if den==0:
			return None
		lvgi=num/den
		return float(lvgi)
	def get_tata(self,df,seed=-1,length=4):
		#tata=(ttmsdfcompany.iloc[-1]['netincomecontinuing']-ttmsdfcompany.iloc[-1]['netcashfromoperatingactivities'])/ttmsdfcompany.iloc[-1]['totalassets']
		netincomecontinuing=self.get_sum_quarters(df,'netincomecontinuing',seed,length)
		netcashfromoperatingactivities=self.get_sum_quarters(df,'netincomecontinuing',seed,length)
		#totalassets=self.get_value(cik,'balance_sheet','totalassets',seed)
		start_assets=self.get_value(df,'totalassets',seed-length)
		end_assets=self.get_value(df,'totalassets',seed)
		if pd.isnull([start_assets,end_assets]).any() or start_assets==0 or end_assets==0:
			return None
		totalassets=pd.Series([start_assets,end_assets]).mean()
		
		if pd.isnull([netincomecontinuing,totalassets,netcashfromoperatingactivities]).any() or totalassets==0:
			return None
		tata=(netincomecontinuing-netcashfromoperatingactivities)/totalassets
		return float(tata)		
	def get_probm(self,df,seed1=-1,seed2=-5,length=4):
		#probmarray=[-4.84,.92*dsri,.528*gmi,.404*aqi,.892*sgi,.115*depi,-1*.172*sgai,-1*.327*lvgi,4.697*tata]
		#https://www.oldschoolvalue.com/blog/investment-tools/beneish-earnings-manipulation-m-score/
		dsri=self.get_dsri(df,seed1,seed2,length)
		gmi=self.get_gmi(df,seed1,seed2,length)
		aqi=self.get_aqi(df,seed1,seed2)
		sgi=self.get_sgi(df,seed1,seed2,length)
		depi=self.get_depi(df,seed1,seed2,length)
		sgai=self.get_sgai(df,seed1,seed2,length)
		lvgi=self.get_lvgi(df,seed1,seed2)
		tata=self.get_tata(df,seed1,length)
		probmarray=[dsri,gmi,aqi,sgi,depi,sgai,lvgi,tata]
		if pd.isnull(probmarray).all():
			return None
		m=[.92,.528,.404,.892,.115,-.172,-.327,4.697]
		s=pd.Series(probmarray)
		m=pd.Series(m)
		probm=s.multiply(m).sum()
		if probm is None:
			return None
		else:
			probm=probm-4.84
		return float(probm)
	def get_pman(self,df,seed1=-1,seed2=-5,length=4):
		probm=self.get_probm(df,seed1,seed2,length)
		if pd.isnull(probm):
			return None
		pman=stats.norm.cdf(probm)
		return float(pman)
		
	def get_mta(self,df,pricesdf,seed=-1):
		#market cap + book value of liabilities
		marketcap=self.get_market_cap(df,pricesdf,seed)
		totalliabilities=self.get_value(df,'totalliabilities',seed)
		if pd.isnull([marketcap,totalliabilities]).any():
			return None
		s=pd.Series([marketcap,totalliabilities])
		m=pd.Series([1,1])
		r=s.multiply(m).sum()
		if pd.isnull(r):
			return None
		mta=float(r)
		return mta
		
	def get_nimta(self,df,prices,seed=-1):
		values=[]
		mtas=[]
		for i in range(seed,seed-4,-1):
			values.append(self.get_value(df,'netincome',i))
			mtas.append(self.get_mta(df,prices,i))
		values=pd.Series(values)
		mtas=pd.Series(mtas)
		values=values/mtas
		m=pd.Series([.5333,.2666,.1333,.0666])
		nimta=values.multiply(m).sum()
		if pd.isnull(nimta):
			return None
		else:
			return float(nimta)
	def get_tlmta(self,df,pricesdf,seed=-1):
		totalliabilities=self.get_value(df,'totalliabilities',seed)
		mta=self.get_mta(df,pricesdf,seed)
		if pd.isnull([mta,totalliabilities]).any() or mta==0:
			return None
		tlmta=totalliabilities/mta
		return float(tlmta)
	def get_cashmta(self,df,pricesdf,seed=-1):
		mta=self.get_mta(df,pricesdf,seed)
		cashandequivalents=self.get_value(df,'cashandequivalents',seed)
		if pd.isnull([mta,cashandequivalents]).any() or mta==0:
			return None
		cashmta=cashandequivalents/mta
		return float(cashmta)
	def get_sigma(self,prices,seed=-1,days=90):
		prices=prices.sort_values('date')
		pctchange=prices['adj_close'].pct_change(periods=252)
		if seed==-1:
			pctchange=pctchange[pd.to_datetime(prices['date']).dt.date>=datetime.today().date()-relativedelta(days=days)]
			sigma=pctchange.std()
			return float(sigma)
		else:
			exit()
	def get_mb(self,df,pricesdf,seed=-1):
		mta=self.get_mta(df,pricesdf,seed)
		totalequityandnoncontrollinginterests=self.get_value(df,'totalequityandnoncontrollinginterests',seed)
		marketcap=self.get_market_cap(df,pricesdf,seed)
		if pd.isnull([marketcap,totalequityandnoncontrollinginterests,mta]).any():
			return None
		den=(totalequityandnoncontrollinginterests+.1*marketcap)
		if pd.isnull(den) or den==0:
			return None
		mb=mta/den
		return float(mb)
	def get_pfd_price(self,pricesdf,seed=-1):
		pricesdf=pricesdf.sort_values('date')
		if seed==-1:
			price=pricesdf['adj_close'].iloc[-1]
		else:
			exit()
		if pd.isnull(price) or price==0:
			return None
		price=float(price)
		if price>15:
			price=float(15)
		price=math.log(price,10)
		return price
	def get_exretavg(self,pricesdf,snpfd,seed=-1):
		pricesdf=pricesdf.sort_values('date')
		pricesdf['adj_close']=pricesdf['adj_close'].astype('float')
		snpfd=snpfd.sort_values('date')
		snpfd['adj_close']=snpfd['adj_close'].astype('float')
		exrets=[]
		if seed==-1:
			end_date=datetime.now().date()
		else:
			exit()
		for i in range(4):#do this 3 times
			start_date=end_date-relativedelta(months=3)
			sp1=snpfd[pd.to_datetime(snpfd['date']).dt.date<=start_date]['adj_close'].iloc[-1]  #self.get_price('$SPX',start_date.strftime('%Y-%m-%d'))
			sp2=snpfd[pd.to_datetime(snpfd['date']).dt.date<=end_date]['adj_close'].iloc[-1]
			c1=pricesdf[pd.to_datetime(pricesdf['date']).dt.date<=start_date]['adj_close']
			if len(c1)==0: #for if the stock has not been around that long...we may want to filter this out before even quering...
				return None
			c1=c1.iloc[-1]
			c2=pricesdf[pd.to_datetime(pricesdf['date']).dt.date<=end_date]['adj_close'].iloc[-1]
			if pd.isnull([sp1,sp2,c1,c2]).any():
				logging.error("None price found")
				return None
			spret=math.log((sp2/sp1),10)
			try:
				cret=math.log((c2/c1),10)
			except:
				return None
			exret=cret-spret
			exrets.append(exret)
			end_date=start_date
		s=pd.Series(exrets)   
		m=pd.Series([.5333,.2666,.1333,.0666])
		exretavg=s.multiply(m).sum()         
		return float(exretavg)
	def get_rsize(self,df,pricesdf,totalmarketcap,seed=-1):
		mc=self.get_market_cap(df,pricesdf,seed)
		if pd.isnull(mc):
			return None
		return float(mc/float(totalmarketcap))
	def get_pfd(self,df,prices,snpfd,totalmarketcap,seed=-1,days=90): #use the last 90 trading days for stocks std.
		nimta=self.get_nimta(df,prices,seed)
		tlmta=self.get_tlmta(df,prices,seed)
		chmta=self.get_cashmta(df,prices,seed)
		sigma=self.get_sigma(prices,seed,days)
		exretavg=self.get_exretavg(prices,snpfd,seed)
		mb=self.get_mb(df,prices,seed)
		price=self.get_pfd_price(prices,seed)
		rsize=self.get_rsize(df,prices,totalmarketcap)
		pfdarray=[nimta,tlmta,chmta,sigma,mb,price,exretavg,rsize]
		if pd.isnull(pfdarray).all():
			return None
		m=[-20.26,1.42,-2.13,1.41,.075,-.058,-7.13,-.045]
		lpfd=pd.Series(pfdarray).multiply(m).sum()-9.16
		try:
			pfd=1/(1+(math.exp(-lpfd)))
			return float(pfd)
		except:
			return None
		
	def get_quickratio(self,df,seed=-1):
		#https://intrinio.com/data-tag/quickratio
		cashandequivalents=self.get_value(df,'cashandequivalents',seed)
		shortterminvestments=self.get_value(df,'shortterminvestments',seed)
		notereceivable=self.get_value(df,'notereceivable',seed)
		accountsreceivable=self.get_value(df,'accountsreceivable',seed)
		totalcurrentliabilities=self.get_value(df,'totalcurrentliabilities',seed)
		s=pd.Series([cashandequivalents,shortterminvestments,notereceivable,accountsreceivable]).astype('float')
		if pd.isnull(s).all() or pd.isnull(totalcurrentliabilities) or totalcurrentliabilities==0:
			return None
		quickratio=(s.sum())/totalcurrentliabilities
		return float(quickratio)
	def get_assetturnover(self,df,seed=-1):
		totalrevenue=self.get_value(df,'totalrevenue',seed)
		totalassets=self.get_balance_sheet_mean_value(df,'totalassets',seed,length=1)
		if pd.isnull([totalrevenue,totalassets]).any() or totalassets==0:
			return None
		assetturnover=totalrevenue/totalassets
		return assetturnover
	def get_netequityovertotalliabilities(self,df,seed=-1):
		totalassets=self.get_value(df,'totalassets',seed)
		totalliabilities=self.get_value(df,'totalliabilities',seed)
		totalequity=(pd.Series([totalassets,totalliabilities]).astype('float').multiply(pd.Series([1,-1]))).sum()
		if pd.isnull([totalequity,totalliabilities]).any() or totalliabilities==0:
			return None
		netequityovertotalliabilities=totalequity/totalliabilities
		return netequityovertotalliabilities
	def get_leverage(self,df,seed=-1):
		longtermdebt=self.get_value(df,'longtermdebt',seed)
		totalassets=self.get_value(df,'totalassets',seed)
		if pd.isnull([longtermdebt,totalassets]).any() or totalassets==0:
			return None
		leverage=longtermdebt/totalassets
		if pd.isnull(leverage):
			return None
		return float(leverage) 
	def get_neqiss(self,df,seed,length):
		issuanceofcommonequity=self.get_sum_quarters(df, 'issuanceofcommonequity', seed, length) #Because this is on the balance sheet, if you give out equity, you have a positive effect THIS IS A POSITIVE VALUE
		repurchaseofcommonequity=self.get_sum_quarters(df, 'repurchaseofcommonequity', seed, length) #THIS IS A NEGATIVE VALUE
		if pd.isnull([issuanceofcommonequity,repurchaseofcommonequity]).any():
			return None
		neqiss=float(issuanceofcommonequity+repurchaseofcommonequity) 
		return float(neqiss)
	def get_currentratio(self,df,seed):
		totalcurrentassets=self.get_value(df,'totalcurrentassets',seed)
		totalcurrentliabilities=self.get_value(df,'totalcurrentliabilities',seed)
		if pd.isnull([totalcurrentassets,totalcurrentliabilities]).any() or totalcurrentliabilities==0:
			return None
		return float(totalcurrentassets)/float(totalcurrentliabilities)
		
	def get_financialstrength(self,df,seed1=-1,seed2=-5,length=4):
		
		#current profitiability
		roa=self.get_roa(df, seed1, length)
		fs_roa=roa>0
		
		fcfta=self.get_cashflowonassets(df,seed=-1,length1=1,length2=4)
		fs_fcfta=fcfta>0
		
		if pd.isnull([roa,fcfta]).any():
			fs_accrual=False
		else:            
			fs_accrual=(fcfta-roa)>0 #change this to use the real values
		
		#stability
		lever1=self.get_leverage(df,seed1)
		lever2=self.get_leverage(df,seed2)
		if pd.isnull([lever1,lever2]).any():
			fs_lever=False
		else:
			fs_lever=lever2-lever1>0
			
		liquid1=self.get_currentratio(df,seed1)
		liquid2=self.get_currentratio(df, seed2)
		if pd.isnull([liquid1,liquid2]).any():
			fs_liquid=False
		else:
			fs_liquid=liquid1-liquid2>0
		
		neqiss1=self.get_neqiss(df, seed1, length)
		neqiss2=self.get_neqiss(df, seed2, length)
		if pd.isnull([neqiss1,neqiss2]).any():
			fs_neqiss=False
		else:
			fs_neqiss=neqiss2-neqiss1>0
		#RECENT OPERATIONAL IMPROVEMENTS
		roa1=self.get_roa(df, seed1, length)
		roa2=self.get_roa(df, seed2, length)
		
		if pd.isnull([roa1,roa2]).any():
			fs_roa2=False
		else:
			fs_roa2=roa1>roa2
		
		fcfta1=self.get_cashflowonassets(df,seed=seed1,length1=1,length2=length)
		fcfta2=self.get_cashflowonassets(df,seed=seed2,length1=1,length2=length)
		if pd.isnull([fcfta1,fcfta2]).any():
			fs_fcfta2=False
		else:
			fs_fcfta2=fcfta1>fcfta2

		margin1=self.get_grossmargin(df,seed=seed1,length=4)
		margin2=self.get_grossmargin(df,seed=seed2,length=4)  
		if pd.isnull([margin1,margin2]).any():
			fs_margin=False
		else:
			fs_margin=margin1>margin2 
				
		at1=self.get_assetturnover(df, seed1)
		at2=self.get_assetturnover(df, seed2)
		if pd.isnull([at1,at2]).any():
			fs_turn=False
		else:
			fs_turn=at1>at2
			  
		fs=[fs_roa,fs_fcfta,fs_accrual,fs_lever,fs_liquid,fs_neqiss,fs_roa2,fs_fcfta2,fs_margin,fs_turn]
		fs=pd.Series(fs)
		financialstrength=float(fs.sum())
		p_fs=financialstrength/float(len(fs))
		return float(p_fs)
	def get_shareissuanceratio(self,df,seed1=-1,seed2=-5,length1=4,length2=4):
		#compare the number of shares repurchased over the past 4 quarters to the 4 quarters before that
		neq1=self.get_neqiss(df,seed1,length1)
		neq2=self.get_neqiss(df,seed2,length2)
		if pd.isnull([neq1,neq2]).any():
			return None
		return (neq1<neq2 and neq1<0) #they are repurchasing and more than last year
	def get_high_price(self,df,period=relativedelta(years=1)):

		if df is None or len(df)==0:
			return None
		df=df[pd.to_datetime(df['date'])>pd.to_datetime(datetime.today().date()-period)]
		if df is None or len(df)==0:
			return None		
		df.loc[:,'cum_split']=1
		if 'split_ratio' not in df.columns:
			df.loc[:,'split_ratio']=1
		for i in range(len(df)-2,-1,-1):
			df.loc[df.index[i],'cum_split']=df.iloc[i]['split_ratio']*df.iloc[i+1]['cum_split']
		df.loc[:,'adj_high']=df['high']/df['cum_split']

		return float(df['adj_high'].max())
	def get_low_price(self,df,period=relativedelta(years=1)):
		if df is None or len(df)==0:
			return None
		df=df[pd.to_datetime(df['date'])>pd.to_datetime(datetime.today().date()-period)]
		if df is None or len(df)==0:
			return None		
		df['cum_split']=1
		if 'split_ratio' not in df.columns:
			df['split_ratio']=1
		for i in range(len(df)-2,-1,-1):
			df.loc[df.index[i],'cum_split']=df.iloc[i]['split_ratio']*df.iloc[i+1]['cum_split']
		df.loc[:,'adj_low']=df['low']/df['cum_split']
		
		return float(df['adj_low'].min())
	def get_volume(self,prices,type="mean",period=relativedelta(months=1)):
		prices=prices[pd.to_datetime(prices['date'])>pd.to_datetime(datetime.today().date()-period)]
		prices=prices.sort_values('date')
		if any(prices['split_ratio'])>1:
			logging.info('split found for:'+str(prices['ticker'].unique()))
			adj_volume_series=pd.Series(data=0,index=prices['date'])
			for i in range(len(prices)):
				sr=(prices['split_ratio'].iloc[:i+1]).product()
				volume=prices['volume'].iloc[i]
				adj_volume=sr*volume
				adj_volume_series.iloc[i]=adj_volume
		else:
			adj_volume_series=prices['volume']
		if type=="mean":
			return adj_volume_series.mean()
		elif type=="sum":
			return adj_volume_series.sum()
		else:
			logging.error('unknown type of return vol')
			return None
	def get_dividend(self,df,seed=-1,length=4):
		dividend=self.get_sum_quarters(df,'paymentofdividends',seed,length)
		total_shares=self.get_value(df,'weightedavedilutedsharesos',seed)
		if pd.isnull([dividend,total_shares]).any():
			return None
		dividend=-dividend/total_shares
		return dividend
	def get_dividend_yield(self,df,prices,seed=-1,length=4,type='mrq'):
		if type=='mrq': #most recent quarter, will multiply by 4 assuming the dividend stays constant
			dividend=self.get_dividend(df,seed,length=1)
			if dividend is None or dividend==0:
				return 0
			else:
				dividend=dividend*4
		elif type=='ttm':
			dividend=self.get_dividend(df,seed,length=4)
		else:
			logging.error('unknown type')
			exit()
		if dividend is None or dividend==0:
			return 0
		prices=prices.sort_values('date')
		price=prices['adj_close'].iloc[-1]
		divyield=dividend/price
		return divyield