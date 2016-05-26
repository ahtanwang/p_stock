#-*- coding: UTF-8 -*- 

import tushare as ts
from sqlalchemy import create_engine
import time
import mylib as me
import pandas as pd
import mydef as md

global G_DBengine



def down_CodeName():
	is_succ = False
	while is_succ == False:
		try:
			global G_CODE
			G_CODE = ts.get_stock_basics()
			del G_CODE['area']	
			del G_CODE['outstanding']
			del G_CODE['totals']	
			del G_CODE['totalAssets']	
			del G_CODE['liquidAssets']	
			del G_CODE['fixedAssets']	
			del G_CODE['reserved']	
			del G_CODE['reservedPerShare']	
			del G_CODE['esp']	
			del G_CODE['bvps']	
			del G_CODE['timeToMarket']	
			
			G_CODE['ndays'] = 0
			
			G_CODE.to_csv('../data/codename.txt')
			is_succ	= True
		except ValueError, e:
			print 'ValueError:', e
			
def down_dk_all(code, i):
	global G_CODE
	is_succ = False
	t_name = 'a' + code
	if me.IsTableExist(t_name, G_DBengine) == False:
		s_date = '2013-01-01'
	else:	
		s_date = me.GetLatestDateFromTable(t_name, G_DBengine)
		
	while is_succ == False:
		try:
			df = ts.get_h_data(code, autype='hfq', start=s_date)
			if str(type(df)) == '<class \'pandas.core.frame.DataFrame\'>': 			
				print s_date, df.index.size
				del df['open']
				del df['high']
				del df['low']
				del df['volume']
				df['amount'] = df['amount'] / 10000 
				df = df.drop(df.index.values[df.index.size-1])
				G_CODE.iat[i,4] = df.index.size
				if df.index.size != 0:
					df.to_sql(t_name, G_DBengine, if_exists='append')
			is_succ	= True
		except ValueError, e:
			print 'ValueError:', e
	
def down_fin(year, season):
	is_succ = False
	t_name = 'zf'+str(year)+str(season)
	print "down_fin.........", t_name
	print t_name
	while is_succ == False:
		try:
			pd = ts.get_profit_data(year,season)
			print type(pd)
			if str(type(pd)) == '<class \'pandas.core.frame.DataFrame\'>': 
				pd.to_sql(t_name, G_DBengine, if_exists='replace')
			is_succ	= True
		except ValueError, e:
			print 'ValueError:', e
		
####################################################################
me.SndEmail(subject='DDK is start.........')
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')	
print me.GetNowTime() + 'start down_CodeName.........'
down_CodeName()	
print 'Download 399300 for hy time line.......... '
df = ts.get_h_data('399300', index=True, start='2013-01-10')
df.to_sql('a399300', G_DBengine, if_exists='replace')
for i in range(G_CODE.index.size):
	print me.GetNowTime() + 'start down_dk_all.........[%d of %d] '%(i,G_CODE.index.size) + G_CODE.index[i]
	down_dk_all(G_CODE.index[i], i)
G_CODE.to_csv('../data/codename.txt')	







