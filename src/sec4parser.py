import xml.etree.cElementTree as et
import urllib2
import pandas as pd
import mongomanager
import logging
import inspect
import requestswrapper
from joblib import Parallel, delayed
import multiprocessing
from random import shuffle

class sec4parser():
	def __init__(self,url=None,xml_text=None):
		#the url should be the sec link to the xbrl xsd or xml file
		if url is not None:
			self.connector=requestswrapper.RequestsWrapper()
			resp=self.connector.issue_request(url)
			if resp is None:
				logging.error('bad requst url:'+url)
				self.root=None
				self.xml_text=None
			else:
				try:
					self.root=et.fromstring(resp.text)
					self.xml_text=resp.text
				except:
					logging.error('parsing error for url:'+url)
					self.root=None
					self.xml_text=None
		elif xml_text is not None:
			try:
				self.root=et.fromstring(xml_text)
				self.xml_text=xml_text
			except:
				logging.error('parsing error for text')
				self.root=None
				self.xml_text=None
		else:
			logging.error(str(url))
			logging.error(str(xml_text))
			exit()
		return
	def get_schema_version(self):
		if self.root is None:
			return None
		return self.root.find('schemaVersion').text
	def get_filing_date(self):
		if self.root is None:
			return None
		periodOfReport=self.root.find('periodOfReport')
		if periodOfReport is None:
			return None
		else:
			return periodOfReport.text
	def get_company_cik(self):
		if self.root is None:
			return None
		issuerCiks=list(self.root.iter("issuerCik"))
		if len(issuerCiks)==0:
			return None
		elif len(issuerCiks)>1:
			logging.error('more than 1 name for owner')
			return None
		else:
			return issuerCiks[0].text
	def get_num_owners(self):
		if self.root is None:
			return None
		return len(self.get_reporting_owners())
	def get_reporting_owners(self):
		if self.root is None:
			return None
		return list(self.root.iter('reportingOwner'))
	def get_owner_relationship(self,reportingOwner,relationship_type="isDirector"):
		if self.root is None:
			return None		
		owner_relationships=list(reportingOwner.iter(relationship_type))
		if len(owner_relationships)==0:
			return None
		elif len(owner_relationships)>1:
			logging.error('more than 1 name for owner')
			return None
		else:
			if owner_relationships[0].text.lower() in ['1','true']:
				return True
			elif owner_relationships[0].text.lower() in ['0','false']:
				return False
			return bool(int(owner_relationships[0].text))
	def get_owner_relationships_info(self,reportingOwner):
		info={}
		info['director']=self.get_owner_relationship(reportingOwner,'isDirector')
		info['officer']=self.get_owner_relationship(reportingOwner,'isOfficer')
		info['ten_percent_owner']=self.get_owner_relationship(reportingOwner,'isTenPercentOwner')
		info['other_relation']=self.get_owner_relationship(reportingOwner,'isOther')
		info['officer_title']=self.get_owner_title(reportingOwner)
		info['owner_cik']=self.get_owner_cik(reportingOwner)
		info['owner_name']=self.get_owner_name(reportingOwner)
		return info
	def get_owner_title(self,reportingOwner):
		if self.root is None:
			return None		
		owner_relationships=list(reportingOwner.iter('officerTitle'))
		if len(owner_relationships)==0:
			return None
		elif len(owner_relationships)>1:
			logging.error('more than 1 name for owner')
			return None
		else:
			return owner_relationships[0].text
	def get_owner_name(self,reportingOwner):
		if self.root is None:
			return None	
		owner_names=list(reportingOwner.iter('rptOwnerName'))
		if len(owner_names)==0:
			return None
		elif len(owner_names)>1:
			logging.error('more than 1 name for owner')
			return None
		else:
			return owner_names[0].text			
	def get_owner_cik(self,reportingOwner):
		if self.root is None:
			return None
		owner_ciks=list(reportingOwner.iter('rptOwnerCik'))
		if len(owner_ciks)==0:
			return None
		elif len(owner_ciks)>1:
			logging.error('more than 1 cik for owner')
			return None
		else:
			return owner_ciks[0].text
	def get_num_nonderivativetransactions(self):
		if self.root is None:
			return None
		return len(self.get_nonderivativetransactions())
	def get_num_derivativetransactions(self):
		if self.root is None:
			return None
		return len(self.get_derivativetransactions())
	def get_nonderivativetransactions(self):
		if self.root is None:
			return None
		return list(self.root.iter('nonDerivativeTransaction'))
	def get_derivativetransactions(self):
		if self.root is None:
			return None
		return list(self.root.iter('derivativeTransaction'))		
	def get_transaction_date(self,nonDerivativeTransaction):
		if self.root is None:
			return None
		transactionDates=list(nonDerivativeTransaction.iter('transactionDate'))
		if len(transactionDates)==0:
			return None
		elif len(transactionDates)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return transactionDates[0].find('value').text
	def get_security_title(self,nonDerivativeTransaction):
		if self.root is None:
			return None
		securityTitles=list(nonDerivativeTransaction.iter('securityTitle'))
		if len(securityTitles)==0:
			return None
		elif len(securityTitles)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return securityTitles[0].find('value').text		
	def get_transaction_type_code(self,nonDerivativeTransaction):
		if self.root is None:
			return None
		transactionCodes=list(nonDerivativeTransaction.iter('transactionCode'))
		if len(transactionCodes)==0:
			return None
		elif len(transactionCodes)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return transactionCodes[0].text	
	def get_ammount_of_shares(self,nonDerivativeTransaction):
		transactionSharess=list(nonDerivativeTransaction.iter('transactionShares'))
		if len(transactionSharess)==0:
			return None
		elif len(transactionSharess)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return float(transactionSharess[0].find('value').text)			
	def get_acquisition_disposition_code(self,nonDerivativeTransaction):
		transactionAcquiredDisposedCodes=list(nonDerivativeTransaction.iter('transactionAcquiredDisposedCode'))
		if len(transactionAcquiredDisposedCodes)==0:
			return None
		elif len(transactionAcquiredDisposedCodes)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return transactionAcquiredDisposedCodes[0].find('value').text				
	def get_transaction_price(self,nonDerivativeTransaction):
		transactionPricePerShares=list(nonDerivativeTransaction.iter('transactionPricePerShare'))
		if len(transactionPricePerShares)==0:
			return None
		elif len(transactionPricePerShares)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			if transactionPricePerShares[0].find('value') is None:
				return None
			else:
				return float(transactionPricePerShares[0].find('value').text)
	def get_total_shares_owned(self,nonDerivativeTransaction):
		sharesOwnedFollowingTransactions=list(nonDerivativeTransaction.iter('sharesOwnedFollowingTransaction'))
		if len(sharesOwnedFollowingTransactions)==0:
			return None
		elif len(sharesOwnedFollowingTransactions)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return float(sharesOwnedFollowingTransactions[0].find('value').text)	
	def get_ownership_type_code(self,nonDerivativeTransaction):
		directOrIndirectOwnerships=list(nonDerivativeTransaction.iter('directOrIndirectOwnership'))
		if len(directOrIndirectOwnerships)==0:
			return None
		elif len(directOrIndirectOwnerships)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return directOrIndirectOwnerships[0].find('value').text
	def get_nonderivative_transaction_info(self,nonDerivativeTransaction):
		info={}
		info['security_title']=self.get_security_title(nonDerivativeTransaction)
		info['transaction_date']=self.get_transaction_date(nonDerivativeTransaction)
		info['transaction_type_code']=self.get_transaction_type_code(nonDerivativeTransaction)
		info['ammount_of_shares']=self.get_ammount_of_shares(nonDerivativeTransaction)
		info['acquisition_disposition_code']=self.get_acquisition_disposition_code(nonDerivativeTransaction)
		info['transaction_price']=self.get_transaction_price(nonDerivativeTransaction)
		info['total_shares_owned']=self.get_total_shares_owned(nonDerivativeTransaction)
		info['ownership_type_code']=self.get_ownership_type_code(nonDerivativeTransaction)
		info['deemed_execution_date']=self.get_deemed_execution_date(nonDerivativeTransaction)
		return info
	def get_expiration_date(self,Transaction):
		expirationDates=list(nonDerivativeTransaction.iter('expirationDate'))
		if len(expirationDates)==0:
			return None
		elif len(expirationDates)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return expirationDates[0].find('value').text
	def get_deemed_execution_date(self,Transaction):
		deemedExecutionDates=list(Transaction.iter('deemedExecutionDate'))
		if len(deemedExecutionDates)==0:
			return None
		elif len(deemedExecutionDates)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			if deemedExecutionDates[0].find('value') is None:
				return None
			else:
				return deemedExecutionDates[0].find('value').text
	def get_exercise_date(self,Transaction):
		exerciseDates=list(nonDerivativeTransaction.iter('exerciseDate'))
		if len(exerciseDates)==0:
			return None
		elif len(exerciseDates)>1:
			logging.error('more than 1 date for transaction')
			return None
		else:
			return exerciseDates[0].find('value').text
	def get_owner_info_list(self):
		if self.root is None:
			return None
		if self.get_reporting_owners() is None or self.get_num_owners()==0 or self.get_num_owners() is None:
			return None
		owner_df=[]
		owners=self.get_reporting_owners()
		for owner in owners:
			owner_df.append(self.get_owner_relationships_info(owner))
		return owner_df
	def get_non_derivative_transactions_list(self):
		if self.root is None:
			return None
		if self.get_num_nonderivativetransactions() is None or self.get_num_nonderivativetransactions()==0:
			return None
		trans_df=[]
		transactions=self.get_nonderivativetransactions()
		for trans in transactions:
			trans_df.append(self.get_nonderivative_transaction_info(trans))
		return trans_df
def allfilings_2_form4(collections,m):

	def get_xml_for_filing(collections,m,totalitems,filing_id):
		if m.db[collections['sec_form4_xmls']].find_one({"_id":filing_id}) is not None:
			return
		filing=m.db[collections['intrinio_filings']].find_one({'_id':filing_id},{'report_url':1})
		report_url=filing['report_url']
		url=report_url.split('/')
		del url[-2]
		url='/'.join(url)
		s=sec4parser(url=url)
		data={}
		data['_id']=filing_id
		data['xml_url']=url
		data['xml_text']=s.xml_text
		m.db[collections['sec_form4_xmls']].update({'_id':data['_id']},data,upsert=True)
		logging.info('complete:'+str(float(m.db[collections['sec_form4_xmls']].count())/float(totalitems)))
		return
	processed_accno=[x['_id'] for x in list(m.db[collections['sec_form4_xmls']].find({},{'_id':1}))]
	available_filings=[x['_id'] for x in list(m.db[collections['intrinio_filings']].find({'report_type':'4'},{"_id":1}))]
	to_process_filings=list(set(available_filings)-set(processed_accno))
	shuffle(to_process_filings)
	totalitems=m.db[collections['intrinio_filings']].find({'report_type':'4'}).count()
	for filing_id in to_process_filings:
		get_xml_for_filing(collections,m,totalitems,filing_id)
def update_data(collections,m):		
	def find_and_update(collections,m,key,function):
		m.create_index(collections['sec_form4_xmls'],key)
		items=m.db[collections['sec_form4_xmls']].find({"$and":[{"xml_text":{"$ne":None}},{"xml_text":{"$exists":True}},{key:{"$exists":False}}]}).batch_size(1)
		for item in items:
			s=sec4parser(xml_text=item['xml_text'])
			item[key]=getattr(s,function)()
			logging.info("updating:"+item['_id']+' for:'+key+' with value:'+str(item[key]))
			m.db[collections['sec_form4_xmls']].update({'_id':item['_id']},item,upsert=True)
		return
	find_and_update(collections,m,'filing_date',"get_filing_date")
	find_and_update(collections,m,'num_owners',"get_num_owners")
	find_and_update(collections,m,'company_cik',"get_company_cik")
	find_and_update(collections,m,'num_derivativetransactions',"get_num_derivativetransactions")
	find_and_update(collections,m,'num_nonderivativetransactions',"get_num_nonderivativetransactions")
	find_and_update(collections,m,'non_derivative_transactions_list',"get_non_derivative_transactions_list")
	find_and_update(collections,m,'owner_info_list','get_owner_info_list')
	
if __name__ == "__main__":
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	allfilings_2_form4()	
	update_data()
