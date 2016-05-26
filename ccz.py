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
Codes = pd.read_csv("..\\data\\qdpm.txt", converters={'code':str})
del Codes['Unnamed: 0']
Codes['iscz'] = False

for i in range(Codes.index.size):	
	code = Codes.loc[i, 'code']
	print me.GetNowTime() + 'read fin  data .........[%d of %d] '%(i,Codes.index.size) , code
	t_name = 'f' + code
	if me.IsTableExist(t_name, G_DBengine) == False:
		print '......' + t_name + ' is not exist....'
		continue
	sql = 'select * from  '	 + t_name 
	df = pd.read_sql_query(sql, G_DBengine)
	if df.index.size < 8 :
		continue
	i_num = df.index.size
	if df.loc[i_num-1, 'nhgdqyl'] < 8:
		continue
	if df.loc[i_num-2, 'nhgdqyl'] < 8:
		continue
	if df.loc[i_num-1, 'sjsrhb'] < 8:
		continue
	if df.loc[i_num-1, 'sjlrhb'] < 10:
		continue
	if df.loc[i_num-2, 'sjsrhb'] < 8:
		continue
	if df.loc[i_num-2, 'sjlrhb'] < 10:
		continue
		
	Codes.loc[i, 'iscz'] = True
	print Codes.loc[i]
	
Codes.to_csv('../data/czg.txt')	

df = Codes[Codes.iscz == True]

df.to_sql('hy900001', G_DBengine, if_exists='replace')


