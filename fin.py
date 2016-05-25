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
import sys


#gongsi fin table
G_NetProfit = 6
G_income = 8
G_year = 10
G_season = 11
G_jzc = 12
G_nhgdqyl = 13
G_jdsr  = 14
G_jdlr  = 15
G_sjsrhb = 16
G_sjlrhb = 17
G_sjsr = 18
G_sjlr = 19

# 1  gongsi; 2 hangye; 9 all, 10 down fin
if len(sys.argv) == 1:
    print 'No input arguments...............'
    exit()
else:    
    opr_type = int(sys.argv[1])

print 'opr_type == ', opr_type    
    
# append for new season
is_append_hy = True
is_debug = False


#global define
global G_DBengine
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')
global Codes
Codes = pd.read_csv("..\\data\\codename.txt", converters={'code':str})
global Fin
if opr_type == 1 or  opr_type == 2 or opr_type == 9:
    Fin = pd.read_sql_table('f20061',G_DBengine)


def down_fin(year, season):
	is_succ = False
	t_name = 'f'+str(year)+str(season)
	print "down_fin.........", t_name
	print t_name
	while is_succ == False:
		try:
			df = ts.get_profit_data(year,season)
			df['year'] = int(year)
			df['season'] = int(season)
			if str(type(df)) == '<class \'pandas.core.frame.DataFrame\'>': 
				df.to_sql(t_name, G_DBengine, if_exists='replace')
			is_succ	= True
		except ValueError, e:
			print 'ValueError:', e

def down_fin_all():
	for year in range(2006,2016):
		for season in range(1,5):
			down_fin(year, season)

def read_fin(year, season):
	global Fin
	t_name = 'f'+str(year)+str(season)
	print t_name
	if me.IsTableExist(t_name, G_DBengine) == False:
		return
	df = pd.read_sql_table(t_name,G_DBengine)
	Fin = Fin.append(df)
	
def fin_read_all():
	y = 2016
	if is_debug:
		y = 2009
	for year in range(2006,y+1):
		for season in range(1,5):
			if year == 2006 and season == 1:
				continue
			read_fin(year, season)
			
def fin_com_gs(code):
	t_name = 'f'+code
	df = Fin[Fin.code == code]
	if df.index.size < 8:
		return
		
	#jdsy jdly
	df.sort_values(['year','season'])
	df = df.dropna(how='any')
	for i in range(0, df.index.size):   #cal
		if df.iat[i,G_season] == 1 or i == 0:	
			df.iat[i,G_jdlr] = df.iat[i,G_NetProfit]	
			df.iat[i,G_jdsr] = df.iat[i,G_income]
		else:
			df.iat[i,G_jdlr] = df.iat[i,G_NetProfit] - df.iat[i-1,G_NetProfit]
			df.iat[i,G_jdsr] = df.iat[i,G_income] - df.iat[i-1,G_income]	
	# nhgdqyl
	if df.index.size >= 3:
		for i in range(3,df.index.size):
			lr_4 = df.iat[i,G_jdlr] + df.iat[i-1,G_jdlr] + df.iat[i-2,G_jdlr] + df.iat[i-3,G_jdlr]
			jzc_4 = df.iat[i,G_jzc] + df.iat[i-1,G_jzc] + df.iat[i-2,G_jzc] + df.iat[i-3,G_jzc]
			df.iat[i,G_nhgdqyl] = lr_4 / jzc_4 * 4 * 100
			if df.iat[i,G_nhgdqyl] > 50:
				df.iat[i,G_nhgdqyl] = 50
			if df.iat[i,G_nhgdqyl] < -50:
				df.iat[i,G_nhgdqyl] = -50
	#sjsr sjlr		
	if df.index.size >= 4:			
		for i in range(4,df.index.size):
			df.iat[i,G_sjlr] = df.iat[i,G_jdlr] + df.iat[i-1,G_jdlr] + df.iat[i-2,G_jdlr] + df.iat[i-3,G_jdlr]
			df.iat[i,G_sjsr] = df.iat[i,G_jdsr] + df.iat[i-1,G_jdsr] + df.iat[i-2,G_jdsr] + df.iat[i-3,G_jdsr]
	#sjhb 
	if df.index.size >= 5:			
		for i in range(5,df.index.size):
			df.iat[i,G_sjlrhb] = (me.MyDiv(df.iat[i,G_sjlr], df.iat[i-1,G_sjlr]) - 1) * 100
			if df.iat[i,G_sjlrhb] > 50:
				df.iat[i,G_sjlrhb] = 50
			if df.iat[i,G_sjlrhb] < -50:
				df.iat[i,G_sjlrhb] = -50
				
			df.iat[i,G_sjsrhb] = (me.MyDiv(df.iat[i,G_sjsr], df.iat[i-1,G_sjsr])- 1) * 100
			if df.iat[i,G_sjsrhb] > 50:
				df.iat[i,G_sjsrhb] = 50
			if df.iat[i,G_sjsrhb] < -50:
				df.iat[i,G_sjsrhb] = -50
			
	
				
	
	df.to_sql(t_name, G_DBengine, if_exists='replace')
	
			
def fin_read_hy(hy):
	is_first = True
	for i in range(Codes.index.size):
		code = Codes.loc[i, 'code']
		t_name = 'f'+code
		if me.IsTableExist(t_name,G_DBengine) == False:
			continue
		if (is_first):
			fin = pd.read_sql_table(t_name,G_DBengine)
			is_first = False
		else:
			df = pd.read_sql_table(t_name,G_DBengine)
			fin = fin.append(df)
		print '...fin_read_hy:' + hy + '.......[%d of %d]'%(i,Codes.index.size) 
	
	return fin	
 
def fin_com_hy(hycode):
	global Fin
	hy_fin = pd.read_sql_table('f600036',G_DBengine)
	del hy_fin['level_0']
	hy_fin['code'] = hycode
	hy_fin['name'] = hycode
	hy_fin['jzc'] = 0.0
	hy_fin['sjsr'] = 0.0
	hy_fin['sjlr'] = 0.0
	hy_fin['sjsrhb'] = 0.0
	hy_fin['sjlrhb'] = 0.0
	i = 0
	for y in range(2006,2017):
		for s in range(1,5):
			df1 = Fin[Fin.year == y]
			df2 = df1[df1.season == s]
			if (df2.index.size == 0):
				i = i + 1
				continue
			d_sum = df2.sum()
			if i < hy_fin.index.size:
				hy_fin.iat[i,G_jzc ] = d_sum.jzc
				hy_fin.iat[i,G_sjsr ] = d_sum.sjsr
				hy_fin.iat[i,G_sjlr ] = d_sum.sjlr
				i= i+1
				
	
	for i in range(3, hy_fin.index.size):
		hy_fin.iat[i,G_sjsrhb ] = hy_fin.iat[i,G_sjsr ] /(hy_fin.iat[i,G_jzc] + hy_fin.iat[i-1,G_jzc] + hy_fin.iat[i-2,G_jzc] + hy_fin.iat[i-3,G_jzc]) * 400
		hy_fin.iat[i,G_sjlrhb ] = hy_fin.iat[i,G_sjlr ] /(hy_fin.iat[i,G_jzc] + hy_fin.iat[i-1,G_jzc] + hy_fin.iat[i-2,G_jzc] + hy_fin.iat[i-3,G_jzc]) * 400
	
	return hy_fin
	
	
#download fin
if opr_type == 10:
    down_fin_all()
			
#read fin data
if opr_type == 1 or opr_type == 9:
	#down_fin(2015, 4)			
	#down_fin(2016, 1)			

	#compyte gongsi fin
	fin_read_all()	
	Fin['jzc'] = 0.0 #12
	Fin['nhgdqyl'] = 0.0 #13
	Fin['jdsr'] = 0.0   #14
	Fin['jdlr'] = 0.0   #15
	Fin['sjsrhb'] = 0.0  #16
	Fin['sjlrhb'] = 0.0  #17
	Fin['sjsr'] = 0.0   #14
	Fin['sjlr'] = 0.0   #15

	for i in range(Fin.index.size):
		Fin.iat[i,G_jzc] = me.MyDiv(Fin.iat[i,6], Fin.iat[i,3]) * 100 + 1


	if is_debug:
		fin_com_gs('000002')
	else:
		t_name_f = ' '
		for i in range(Codes.index.size):	
			code = Codes.loc[i, 'code']
			print me.GetNowTime() + 'start fin compute.........[%d of %d] '%(i,Codes.index.size) , code
			t_name_f = 'f'+code
			fin_com_gs(code)


#compute hy fin
if opr_type == 2 or opr_type == 9:
	for i in range(len(md.HYL)):
		t_name = 'hy' + md.HYL[i][0]
		if me.IsTableExist(t_name,G_DBengine) == False:
			print '[Warning] %s is not exist.....'%t_name
			continue
		if me.IsTableExist('fhy'+md.HYL[i][0], G_DBengine) == True and is_append_hy == True:
			print '[Warning] %s is alread exist for Append mode.....'%md.HYL[i][0]
			continue		
		Codes = pd.read_sql_table(t_name,G_DBengine)
		Fin = fin_read_hy(md.HYL[i][1])
		print md.HYL[i][1] + ' size ....', Fin.index.size
		hy_fin = fin_com_hy(md.HYL[i][0])
		hy_fin.to_sql('fhy'+md.HYL[i][0], G_DBengine, if_exists='replace')
		
