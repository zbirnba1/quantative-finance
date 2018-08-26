import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pygsheets
import logging
import os

def pgs_get_spreadsheets(service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	allsheets=pd.DataFrame(gs.list_ssheets())
	return allsheets
def pgs_get_worksheets(spreadsheetid,service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	sh=gs.open_by_key(spreadsheetid)
	worksheets=sh.worksheets()
	df=pd.DataFrame()
	for ws in worksheets:
		data={}
		data['id']=ws.id
		data['index']=ws.index
		data['title']=ws.title
		data['rows']=ws.rows
		data['cols']=ws.cols
		df=df.append(data,ignore_index=True)
	return df
def pgs_get_spreadsheet(spreadsheetid,service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	sh=gs.open_by_key(spreadsheetid)
	return sh
def pgs_get_worksheet(spreadsheetid,worksheetid,service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	sh=gs.open_by_key(spreadsheetid)
	ws=sh.worksheet('id',worksheetid)
	return ws
def pgs_worksheetname2id(spreadsheetid,worksheetname,service_file=None):
	worksheets=pgs_get_worksheets(spreadsheetid,service_file)
	worksheets=worksheets[worksheets['title']==worksheetname]
	return worksheets[worksheets['title']==worksheetname]['id'].iloc[0]
def pgs_is_valid_spreadsheet(spreadsheetid,service_file=None):
	spreadsheets=pgs_get_spreadsheets(service_file)
	return spreadsheetid in list(spreadsheets['id'])
def pgs_is_valid_worksheet(spreadsheetid,worksheetname,service_file=None):
	worksheets=pgs_get_worksheets(spreadsheetid,service_file)
	worksheets=worksheets[worksheets['title']==worksheetname]
	if len(worksheets)==0:
		return False
	else:
		return True
def pgs_create_new_spreadsheet(title,service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	sh=gs.create(title)
	return sh
def pgs_create_new_worksheet(spreadsheetid,worksheetname,service_file=None):
	gs=pygsheets.authorize(service_file=service_file)
	sh=pgs_get_spreadsheet(spreadsheetid,service_file)
	ws=sh.add_worksheet(worksheetname,rows=1,cols=1)
	return ws
def pgs_get_worksheet_as_df(spreadsheetid,worksheetname,service_file=None):
	if not pgs_is_valid_spreadsheet(spreadsheetid=spreadsheetid,service_file=service_file) or not pgs_is_valid_worksheet(spreadsheetid=spreadsheetid,worksheetname=worksheetname,service_file=service_file):
		logging.error('spreadsheetid or worksheet is not valid')
		return None
	wsid=pgs_worksheetname2id(spreadsheetid=spreadsheetid,worksheetname=worksheetname,service_file=service_file)
	ws=pgs_get_worksheet(spreadsheetid=spreadsheetid,worksheetid=wsid,service_file=service_file)
	df=ws.get_as_df(has_header=True, index_colum=None, start=None, end=None, numerize=True, empty_value='')
	return df
def pgs_upload_df_to_worksheet(df,spreadsheetid,worksheetname,service_file=None,cell_properties=pd.DataFrame()):

	if not pgs_is_valid_spreadsheet(spreadsheetid=spreadsheetid,service_file=service_file):
		logging.error('invalud spreadsheet id')
		return None
	if not pgs_is_valid_worksheet(spreadsheetid=spreadsheetid,worksheetname=worksheetname,service_file=service_file):
		pgs_create_new_worksheet(spreadsheetid=spreadsheetid,worksheetname=worksheetname,service_file=service_file)
	ws_id=pgs_worksheetname2id(spreadsheetid,worksheetname,service_file=service_file)
	ws=pgs_get_worksheet(spreadsheetid=spreadsheetid,worksheetid=ws_id,service_file=service_file)
	sh=pgs_get_spreadsheet(spreadsheetid, service_file=service_file)
	if len(df)==0:
		logging.info('df is none, removing worksheet')
		sh.del_worksheet(ws)
	if df.index.name!=None: #if the index actually has a name, the we add it to the frame and reset the index
		df=df.reset_index(drop=False)
	df = df.fillna(value='') #remove any nans and just leave them blank
	ws.resize(len(df)+1,len(df.columns))
	cell_list=[]
	#first we do the columns
	for col in df.columns:
		cell=pygsheets.Cell((1,1+df.columns.get_loc(col)),val=col)
		cell_list.append(cell)
	#Now we do the data
	for index,data in df.iterrows():
		data=data.to_dict()
		row=df.index.get_loc(index)+2 #because we have the header row
		for key in data:
			col=df.columns.get_loc(key)+1
			val=data[key]
			cell=pygsheets.Cell((row,col),val=val)

			if len(cell_properties)>0:
				if key not in cell_properties.columns:
					continue
				cell_properties_row=df.index.get_loc(index)
				cell_properties_col=cell_properties.columns.get_loc(key)
				cell_property=cell_properties.iloc[cell_properties_row,cell_properties_col][0]
				if 'color' in cell_property:
					cell.color=cell_property['color']
			cell_list.append(cell)
	ws.update_cells(cell_list=cell_list)
	return

def chunks(l, n):
	# For item i in a range that is a length of l,
	for i in range(0, len(l), n):
		# Create an index range for l of n items:
		yield l[i:i+n]
def create_new_sheet(sheetname='stock_performance',keyfile=None):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)
	sh=gc.create(sheetname)
	return sh.id
def list_permissions(sheetid,keyfile=None):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)
	sh=gc.open_by_key(sheetid)
	return pd.DataFrame(sh.list_permissions())
def add_permission(id,email,notify,perm_type,role,keyfile=None):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)	
	gc.insert_permission(id,email,perm_type,role,notify)
	return

def uploaddf2sheet(df,sheetid,worksheet,keyfile=None,chunksize=5):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)
	sh=gc.open_by_key(sheetid)
	titles=[x.title for x in sh.worksheets()]
	if worksheet not in titles:
		sh.add_worksheet(worksheet, 1, 1)
	if "Sheet1" in titles:
		sh.del_worksheet(sh.worksheet("Sheet1"))
	ws=sh.worksheet(worksheet)
	if df.index.name!=None:
		df=df.reset_index(drop=False)	
	rows=len(df)
	cols=len(df.columns)	
	worksheet = ws
	worksheet.resize(len(df)+1,len(df.columns))
	rng0=worksheet.range(1,1,1,cols)
	for cell in rng0:
		row=cell.row
		col=cell.col
		cell.value=df.columns[col-1]
	rng=worksheet.range(2,1,rows+1,cols)
	for cell in rng:
		row=cell.row
		col=cell.col
		cell.value=df.iloc[row-2,col-1]
	if chunksize is None:
		chunksize=len(rng0+rng)
	for chunk in list(chunks(rng0+rng,chunksize)): #upload in smaller chunks
		result=False
		while result is False:
			result=upload_chunk(worksheet,chunk)
	return True
def downloadsheet2df(sheetid,worksheet,keyfile=None):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)
	sh=gc.open_by_key(sheetid)
	ws=sh.worksheet(worksheet)	
	df=pd.DataFrame(ws.get_all_records())
	return df


def get_allids(keyfile=None):
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
	gc = gspread.authorize(credentials)
	ids=[x.id for x in gc.openall()]
	return ids
def downloadfromgooglesheets(spreadsheetname,worksheetname):
	json_key = None
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	gc = gspread.authorize(credentials)
	all_sheets=gc.openall()
	sheetname=spreadsheetname
	sheetfound=False
	for sheet in all_sheets:
		if sheet.title==sheetname:
			sh=gc.open(sheetname)
			sheetfound=True
	if sheetfound is False:
		return pd.DataFrame()
	worksheets=sh.worksheets()
	worksheetname=worksheetname
	worksheetfound=False
	for worksheet in worksheets:
		if worksheet.title==worksheetname:
			ws=sh.worksheet(worksheetname)
			worksheetfound=True
	if worksheetfound is False:
		return pd.DataFrame()
	df=pd.DataFrame(ws.get_all_records())
	return df
def get_update_time(spreadsheetname,worksheetname):
	json_key = None
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	gc = gspread.authorize(credentials)
	all_sheets=gc.openall()
	sheetname=spreadsheetname
	sheetfound=False
	for sheet in all_sheets:
		if sheet.title==sheetname:
			sh=gc.open(sheetname)
			sheetfound=True
	if sheetfound is False:
		return None
	worksheets=sh.worksheets()
	worksheetname=worksheetname
	worksheetfound=False
	for worksheet in worksheets:
		if worksheet.title==worksheetname:
			ws=sh.worksheet(worksheetname)
			worksheetfound=True
	if worksheetfound is False:
		return None
	time=ws.updated
	return time
def delete_worksheet(spreadsheetname,worksheetname):
	json_key = None
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	gc = gspread.authorize(credentials)
	sh=gc.open(spreadsheetname)
	ws=sh.worksheet(worksheetname)
	sh.del_worksheet(ws)
	return
def create_sheet(spreadsheetname,worksheetname,email='birnbaum.zachary@gmail.com'):
	json_key = None
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key, scope)
	gc = gspread.authorize(credentials)
	all_sheets=gc.openall()
	sheetname=spreadsheetname
	sheetfound=False
	for sheet in all_sheets:
		return
	if sheetfound is False:
		sh=gc.create(sheetname)
		sh.share(email,perm_type='user',role='owner') #set some permissions
	worksheets=sh.worksheets()
	worksheetname=worksheetname
	worksheetfound=False
	for worksheet in worksheets:
		if worksheet.title==worksheetname:
			return
	if worksheetfound is False:
		ws=sh.add_worksheet(worksheetname,1,1)
	return True
def upload_chunk(worksheet,chunk):
	try:
		worksheet.update_cells(chunk)
		return True
	except Exception as e:
		logging.error(e)
		exit()
		return False
if __name__ == '__main__':
	pass