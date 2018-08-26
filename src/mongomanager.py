from pymongo import MongoClient
import logging
import inspect
import json

class MongoManager: #what all the scripts will use to connect to the dbs, so now its not in every single script, this can also auto add the indxes, do the updates, return things, etc.
	def __init__(self,dbname,host='localhost',port=27017,username=None,password=None):
		client = MongoClient(host, port)
		db=client[dbname]
		self.client=client
		if username is not None and password is not None:
			db.authenticate(username, password, mechanism='SCRAM-SHA-1')
		elif username is None and password is None:
			pass
		else:
			logging.error('no username and password specified')
			exit()
		self.db=db				
		return
		
	def reindex_collections(self,collections=None):
		if collections is None:
			collections=self.db.collection_names()
		for collection in collections:
			logging.info("reindexing:"+collection)
			self.db[collection].reindex()
			logging.info('done reindexing:'+collection)
		return
		
	def backup_db(self,destinationfolder='.',collections=None):
		if collections is None:
			collections=self.db.collection_names()
		#backup the db to your local harddrive
		for collection in collections:
			logging.info('backing up collection: '+collection)
			file=open(destinationfolder+'/'+collection+'.json','w')
			items=self.db[collection].find().batch_size(100)
			for item in items:
				try:
					jsonitem=json.dumps(item)
					file.write(jsonitem)
					file.write('\n')
				except Exception as e:
					logging.error(e)
					logging.error(item)
					continue

			file.close()
		return
	def create_collections(self,collections):
		for collection in collections:
			if collection not in self.get_collections():
				self.create_collection(collection)
				logging.info('collection created: '+collection)
			else:
				logging.info('collection already exists: '+collection)
		return
	def drop_collections(self,collections):
		for collection in collections:
			if collection in self.get_collections():
				self.db.drop_collection(collection)
		return
	def create_collection(self,collection):
		if collection not in self.get_collections():
			self.db.create_collection(collection)
		return
	def get_collections(self):
		return self.db.collection_names()
	def index_information(self,collection):
		return self.db[collection].index_information()
	def create_index(self,collection,index,name=None,unique=False,background=True):
		currentindexes=self.index_information(collection)
		if name==None:
			name=index
		if name not in currentindexes:
			self.db[collection].create_index(index,name=name,unique=unique,background=background)
			logging.info('new index created: '+index)
		else:
			pass
			#logging.info('index already exists: '+index)
		return
	def drop_indexes(self,collection,indexname=None):
		if indexname is None:
			todrop=self.index_information(collection)
		else:
			todrop=[indexname]
		for index in todrop:
			self.db[collection].drop_index(index)
		return
	def get_keys(self,collection):
		keys=set()
		for item in self.db[collection].find():
			keys=keys.union(set(item.keys()))
		return list(keys)
if __name__ == '__main__':
	logging.basicConfig(filename=inspect.stack()[0][1].replace('py','log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
	# m=MongoManager(host='financedevel')
	# x=m.index_information('companies')