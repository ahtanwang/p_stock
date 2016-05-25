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
import time


global G_DBengine
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')

global Codes
Codes = pd.read_csv("..\\data\\czg.txt", converters={'code':str})

global F_one
F_bear = open('..\\data\\bear.txt',"w")
F_one = open('..\\data\\one.txt',"w")


F_ONEH = open('C:\\zd_zszq\\T0002\\blocknew\\oneh.blk',"w")
F_ONES = open('C:\\zd_zszq\\T0002\\blocknew\\ones.blk',"w")
F_BEAR = open('C:\\zd_zszq\\T0002\\blocknew\\bear.blk',"w")

is_mail = True

#one hold
for i in range(Codes.index.size):	
	gs = Codes.loc[i, 'code']
	if gs in md.ONE_HL:
		t_name = 'b' + gs
		sql = 'select * from  '	 + t_name 
		df = pd.read_sql_query(sql, G_DBengine)
		me.PinghuaDF(df, md.BI_syl30+1, 5)
		me.PinghuaDF(df, md.BI_syl250+1, 30)
		num = df.index.size
		if df.loc[num-1, 'syl250'] < 0:
			tag_250 = '-'
		elif df.loc[num-1, 'syl250'] > 50:
			tag_250 = '*'
		else:		
			tag_250 = '+'

		F_one.write('%7s'%gs)
		F_one.write('%10s'%Codes.loc[i, 'name'])		
		F_one.write('%6s'%str(df.loc[num-1, 'date'])[5:10])	
		F_one.write('%2s '%tag_250)	
		
		for k in range(40, -1, -1):		
			if df.loc[num-1-k, 'syl30'] < 0:
				tag_30 = 'B'
			elif df.loc[num-1-k, 'syl30'] > 12:
				tag_30 = '-'
			else:		
				tag_30 = '.'
			F_one.write('%s'%tag_30)
		F_one.write('[%6.2f]\n'%df.loc[num-1, 'syl30'])

		if int(gs) > 599999:
			F_ONEH.write('1%s\n'%gs)
		else:
			F_ONEH.write('0%s\n'%gs)
		
F_one.write('\n')		
F_one.write('\n')

		
for i in range(Codes.index.size):	
	gs = Codes.loc[i, 'code']
	if gs in md.ONE_HS:
		t_name = 'b' + gs
		sql = 'select * from  '	 + t_name 
		df = pd.read_sql_query(sql, G_DBengine)
		me.PinghuaDF(df, md.BI_syl30+1, 5)
		me.PinghuaDF(df, md.BI_syl250+1, 30)
		num = df.index.size
		F_one.write('%7s'%gs)	
		F_one.write('%10s'%Codes.loc[i, 'name'])
		F_one.write('%6s '%str(df.loc[num-1, 'date'])[5:10])	
		
		for k in range(30, -1, -1):				
			if df.loc[num-1-k, 'syl250'] < 0:
				tag_250 = '-'
			elif df.loc[num-1-k, 'syl250'] > 50:
				tag_250 = '*'
			else:		
				tag_250 = '+'

			F_one.write('%s'%tag_250)	
		F_one.write('[%6.2f]\n'%df.loc[num-1, 'syl250'])
		
		if int(gs) > 599999:
			F_ONES.write('1%s\n'%gs)
		else:
			F_ONES.write('0%s\n'%gs)

			
#### bear.txt
for i in range(Codes.index.size):	
	gs = Codes.loc[i, 'code']
	if Codes.loc[i, 'qdpm'] < 70 and gs not in md.BEAR_HL:
		continue
	if Codes.loc[i, 'iscz'] == False and gs not in md.BEAR_HL:
		continue
	t_name = 'b' + gs
	sql = 'select * from  '	 + t_name 
	df = pd.read_sql_query(sql, G_DBengine)
	me.PinghuaDF(df, md.BI_syl30+1, 5)
	me.PinghuaDF(df, md.BI_syl250+1, 30)
	num = df.index.size
	if df.loc[num-1, 'syl250'] < 0:
		tag_250 = '-'
	elif df.loc[num-1, 'syl250'] > 50:
		tag_250 = '*'
	else:		
		tag_250 = '+'
	if tag_250 == '-':
		continue
	F_bear.write('%7s'%gs)	
	F_bear.write('%10s'%Codes.loc[i, 'name'])
	F_bear.write('%6s'%str(df.loc[num-1, 'date'])[5:10])	
	F_bear.write('%2s'%tag_250)	
	if gs  in md.BEAR_HL:
		F_bear.write('[H] ')	
	elif gs  in md.BEAR_BLACK:
		F_bear.write('[X]\n')
		continue	
	else:
		F_bear.write('[ ] ')	
	
	for k in range(30, -1, -1):		
		if df.loc[num-1-k, 'syl30'] < 0:
			tag_30 = 'B'
		elif df.loc[num-1-k, 'syl30'] > 15:
			tag_30 = '-'
		else:		
			tag_30 = '.'
		F_bear.write('%s'%tag_30)
	F_bear.write('[%6.2f]\n'%df.loc[num-1, 'syl30'])

	if int(gs) > 599999:
		F_BEAR.write('1%s\n'%gs)
	else:
		F_BEAR.write('0%s\n'%gs)
		
F_one.close()
F_ONES.close()
F_ONEH.close()
F_bear.close()
F_BEAR.close()

if is_mail:
	me.SndEmail(subject='One is OK.........', filename='..\\data\\one.txt')	
	time.sleep(10)
	me.SndEmail(subject='Bear is OK.........', filename='..\\data\\bear.txt')			