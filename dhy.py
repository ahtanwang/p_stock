#-*- coding: UTF-8 -*- 

import tushare as ts
from sqlalchemy import create_engine
import time
import mylib as me
import mydef as md
import pandas as pd

global G_DBengine

####################################################################
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')	
print me.GetNowTime() + 'start down hy info.........'

print 'hy300 ............................'
df = ts.get_hs300s()
df.to_sql('hy300', G_DBengine, if_exists='replace')

print 'hy101 ............................'
df = ts.get_sme_classified()
df.to_sql('hy101', G_DBengine, if_exists='replace')

print 'hy102 ............................'
df = ts.get_gem_classified()
df.to_sql('hy102', G_DBengine, if_exists='replace')


df = pd.read_csv("..\\data\\index.txt", converters={'code':str,'c_code':str}, encoding='gbk')
for i in range(len(md.HYL)):
	t_name = 'hy' + md.HYL[i][0]
	if t_name == 'hy300' or  t_name == 'hy101' or t_name == 'hy102':
		continue
	print '%s ............................'%t_name
	df1 = df[df.c_code == md.HYL[i][0]]
	if df1.index.size == 0:
		continue
	df1.to_sql(t_name, G_DBengine, if_exists='replace')
	





