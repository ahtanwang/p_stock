#-*- coding: UTF-8 -*- 
import pylab
import pandas as pd    
import MySQLdb
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import tushare as ts
import numpy as np
import mylib as me

#if compute all and init database
Is_ComForAll = False

#if for debug, just compute one gongsi
Is_Debug = False

global G_DBengine
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')
global Codes
Codes = pd.read_csv("..\\data\\codename.txt", converters={'code':str})




def Comp_gs_QD30(df, num_b):
	if df.index.size < 125:
		return 
	for i in range(120, df.index.size):
		if df.iat[i-120,11] != 0:
			qd = (me.MyDiv( df.iat[i,11], df.iat[i-120,11]) - 1) * 100
			df.iat[i,3] = qd


def Comp_gs_syl30(df, num_b):
	if df.index.size < 35:
		return 
	i_end = df.index.size	
	if num_b == 0:
		i_begin = 30
	else:
		i_begin = i_end - (i_end - num_b)
	for i in range(i_begin, df.index.size): 
		money = 0.0
		sy = 0.0
		syl = 0.0
		for j in range(i-30,i+1):
			sy = sy + (df.iat[i,1] /  df.iat[j,1] -1 ) * df.iat[j,2]
			money = money + df.iat[j,2]
		syl = sy / money * 100
		df.iat[i,6] = sy
		df.iat[i,4] = money
		df.iat[i,8] = syl
	#Com_pinghua(df, 10, 8, 5)
	
def Comp_gs_p30d2(df, num_b):
	if df.index.size < 35:
		return 
	for i in range(30, df.index.size): 
		p_total = 0.0
		for j in range(i-30,i+1):
			p_total = p_total + df.iat[j,1]
		p30 = p_total / 30
		df.iat[i,11] = p30
		
	d = 0.0
	for i in range(df.index.size):
		if df.iat[i,11] == 0:
			continue
		if df.iat[i,1] >= df.iat[i,11]:
			if d < 0:
				d = 0
			d = d + 1
			df.iat[i,12] = d * d
		else:
			if d > 0:
				d = 0
			d = d - 1
			df.iat[i,12] = d * d * (-1)
			
def Comp_gs_p120d2(df, num_b):
	if df.index.size < 125:
		return 
	for i in range(120, df.index.size): 
		p_total = 0.0
		for j in range(i-120,i+1):
			p_total = p_total + df.iat[j,1]
		pa = p_total / 120
		df.iat[i,13] = pa
		
	d = 0.0
	for i in range(df.index.size):
		if df.iat[i,13] == 0:
			continue
		if df.iat[i,1] >= df.iat[i,13]:
			if d < 0:
				d = 0
			d = d + 1
			df.iat[i,14] = d * d
		else:
			if d > 0:
				d = 0
			d = d - 1
			df.iat[i,14] = d * d * (-1)			
	
def Comp_gs_syl250(df, num_b):
	if df.index.size < 255:
		return 
	i_end = df.index.size	
	if num_b == 0:
		i_begin = 250
	else:
		i_begin = i_end - (i_end - num_b)
	for i in range(i_begin, df.index.size): 
		money = 0.0
		sy = 0.0
		syl = 0.0
		for j in range(i-250,i+1):
			sy = sy + (df.iat[i,1] /  df.iat[j,1] -1 ) * df.iat[j,2]
			money = money + df.iat[j,2]
		syl = sy / money * 100
		df.iat[i,7] = sy
		df.iat[i,5] = money
		df.iat[i,9] = syl
		
def Com_pinghua(df, from_i, to_i, days):
	for i in range(days, df.index.size): 
		total = 0.0
		for j in range(i-days,i+1):
			total = total + df.iat[j,from_i]
		total = total / days
		df.iat[i,to_i] = total


def Comp_gs(code):
	t_name_a  = 'a' + code 
	t_name_b  = 'b' + code
	if me.IsTableExist(t_name_a, G_DBengine) == False:
		return
###		
	sql = 'select * from  '	 + t_name_a 
	df = pd.read_sql_query(sql, G_DBengine)
	#df = pd.read_sql_table(t_name_a,G_DBengine)
###	
	num_a = df.index.size
	if me.IsTableExist(t_name_b, G_DBengine) == False:
		num_b = 0
	else:
		if Is_ComForAll:
			num_b = 0
		else:	
			num_b = me.GetNumFromTable(t_name_b, G_DBengine)
	if (num_a == num_b):
		print "[Warning]Comp_gs no new data............" + code
		return
	else:
		print "[.......]Comp_gs will add %d days data............"%(num_a-num_b)
	df = df.sort_values('date', ascending=True)
	# date 0, close 1, money 2  
	df['qd30'] = 0.0 #3
	df['je30'] = 0.0 #4
	df['je250'] = 0.0 #5
	df['sy30'] = 0.0 #6
	df['sy250'] = 0.0 #7
	df['syl30'] = 0.0 #8
	df['syl250'] = 0.0 #9
	df['tmp'] = 0.0 #10
	df['p30'] = 0.0  #11
	df['p30d2'] = 0.0  #12
	df['p120'] = 0.0  #13
	df['p120d2'] = 0.0  #14	
	Comp_gs_syl30(df, num_b)
	Comp_gs_syl250(df, num_b)
	Comp_gs_p30d2(df, num_b)
	Comp_gs_p120d2(df, num_b)
	Comp_gs_QD30(df, num_b)
	
	if Is_ComForAll:
		df.to_sql(t_name_b, G_DBengine, if_exists='replace')	
	else:
		if (num_b != 0):
			df = df.drop(range(num_b))
		df.to_sql(t_name_b, G_DBengine, if_exists='append')
	
################main
if Is_Debug:
	Comp_gs('600519')
else:	
	#me.SndEmail([],subject='CBASE is starting.........')
	for i in range(Codes.index.size):	
		code = Codes.loc[i, 'code']
		print me.GetNowTime() + 'start comput.........[%d of %d] '%(i,Codes.index.size) , code
		Comp_gs(code)
	me.SndEmail(subject='CBASE is over.........')	

