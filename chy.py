#-*- coding: UTF-8 -*- 
import pylab
import pandas as pd    
import MySQLdb
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import tushare as ts
import numpy as np
import mylib as me
import mydef as md

global G_DBengine
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')

global Codes
Codes = pd.read_csv("..\\data\\codename.txt", converters={'code':str})

global Hycode
Hycode = pd.read_sql_table('hy300',G_DBengine)

global Hytime 
Hytime = pd.read_sql_table('a399300',G_DBengine)
Hytime =  Hytime.sort_values('date', ascending=True)

global HYdf
HYdf = pd.read_sql_query('select * from b600036 where date=\'2016-04-29\'',G_DBengine)
HYdf = HYdf.drop(0)

def Chy_Com_hy(hyname):
	global HYdf
	global Hycode
	is_table_exist = False
	t_name = 'hyb' + hyname
	if me.IsTableExist(t_name, G_DBengine) == True:
		is_table_exist = True
		
	for i in range(Hytime.index.size):
		date2 = Hytime.iat[i, 0]
		print 'Chy_Com_hy.....%s...'%hyname, Hycode.index.size, date2
		if is_table_exist:
			if me.Is_dateInTable(t_name, G_DBengine, date2) == True:
				continue
		Chy_date(date2)	
		HYdf.to_sql(t_name, G_DBengine, if_exists='append')
		is_table_exist = True	
		HYdf = HYdf.drop(0)
		
def Chy_date(date2):
	global HYdf
	global Hycode
	is_first = True
	for i in range(Hycode.index.size):	
		code = Hycode.loc[i, 'code']
		t_name = 'b'+code
		if me.IsTableExist(t_name, G_DBengine) == False:
			continue
		sql = 'select * from  ' + t_name + ' where date =' +  '\'' + str(date2) + '\''
		df = pd.read_sql_query(sql, G_DBengine)
		if is_first	 == True and df.index.size != 0:
			df_t = df
			HYdf = HYdf.append(df) ####################
			is_first = False
		elif df.index.size != 0:
			df_t = df_t.append(df)
			
	dm = df_t.mean()
	ds = df_t.sum()
	ind = HYdf.index.size - 1
	HYdf.iat[ind,md.BI_je30+1] = ds.je30
	HYdf.iat[ind,md.BI_je250+1] = ds.je250
	HYdf.iat[ind,md.BI_sy30+1] = ds.sy30
	HYdf.iat[ind,md.BI_sy250+1] = ds.sy250
	if ds.je30 != 0:
		HYdf.iat[ind,md.BI_syl30+1] =  (ds.sy30 / ds.je30) * 100
	if ds.je250 != 0:
		HYdf.iat[ind,md.BI_syl250+1] = (ds.sy250 / ds.je250) * 100
	HYdf.iat[ind,md.BI_p30d2+1] = dm.p30d2
	HYdf.iat[ind,md.BI_p120d2+1] = dm.p120d2

##########################################################	
for i in range(len(md.HYL)):
	t_name = 'hy' + md.HYL[i][0]
	if me.IsTableExist(t_name,G_DBengine) == False:
		print '[Warning] %s is not exist.....'%t_name
		continue
	Hycode = pd.read_sql_table(t_name,G_DBengine)
	if Hycode.index.size == 0:
		continue
	Chy_Com_hy(md.HYL[i][0])
