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
Codes['qd30'] = 0.0
Codes['qdpm'] = 0.0

for i in range(Codes.index.size):	
	code = Codes.loc[i, 'code']
	print me.GetNowTime() + 'read qd  data .........[%d of %d] '%(i,Codes.index.size) , code
	t_name = 'b' + code
	if me.IsTableExist(t_name, G_DBengine) == False:
		Codes.loc[i, 'qd30'] = -99.99
		print '......' + t_name + ' is not exist....'
		continue
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 1'
	df = pd.read_sql_query(sql, G_DBengine)
	Codes.loc[i, 'qd30'] = df.loc[0,'qd30']
Codes = Codes.sort_values('qd30', ascending=False)
for i in range(Codes.index.size):	
	Codes.iat[i, 7] = 100 - float(i) / Codes.index.size * 100
Codes.to_csv('../data/qdpm.txt')	

