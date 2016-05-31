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

global Index
Index = pd.read_csv("..\\data\\index.txt", converters={'code':str, 'c_code':str})

global Qdpm
Qdpm = pd.read_csv("..\\data\\qdpm.txt", converters={'code':str})

F_dp = open('..\\data\\dp.txt',"w")
F_hy = open('..\\data\\hy.txt',"w")
F_dp_l = open('..\\data\\dp_l.txt',"w")
F_hy_l = open('..\\data\\hy_l.txt',"w")


is_mail = True

sql = 'SELECT * FROM  hyb300 ORDER BY date DESC  limit 50'
DF = pd.read_sql_query(sql, G_DBengine)
del DF['level_0']
del DF['index']
DF =  DF.sort_values('date')
me.PinghuaDF(DF, md.BI_syl30, 5)
me.PinghuaDF(DF, md.BI_p120d2, 5)
me.PinghuaDF(DF, md.BI_p30d2, 5)
me.PinghuaDF(DF, md.BI_syl250, 30)
DF['code'] = '300'
DF['name'] = '399300'
DATE = DF.iat[49, md.BI_date]

#########################read data###################################
for i in range(1, len(md.HYL)):
	hycode = md.HYL[i][0]
	hyname = md.HYL[i][1]
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 50'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	df['code'] = hycode
	df['name'] = hyname
	DF = DF.append(df)
	
DF = DF[DF.date == DATE]
DF = DF.sort_values('p30d2', ascending=False)

print DF[['syl250','syl30', 'p30d2', 'p120d2', 'name']]
######################################## dp.txt #######################
F_dp.write('code [p120d  p30d]  [syl250  syl30]\n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	if int(hycode) < 1000:
		F_dp.write('%4s%10s'%(DF.iat[i, md.BI_p120d2+1], str(DATE)[5:10]))
	
		if DF.iat[i, md.BI_p120d2] < 0:
			t_p120d2 = '-'
		else :	
			t_p120d2 = '+'
		
		if DF.iat[i, md.BI_p30d2] < 0:
			t_p30d2 = '-'
		else :	
			t_p30d2 = '+'
			
		if 	DF.iat[i, md.BI_syl250] < 0:
			t_syl250 = '-'
		elif DF.iat[i, md.BI_syl250] > 40:
			t_syl250 = '*'
		else:
			t_syl250 = '+'

		if 	DF.iat[i, md.BI_syl30] < 0:
			t_syl30 = '-'
		else:
			t_syl30 = '+'

		F_dp.write('  [%2s %2s]  [%2s %2s]\n'%(t_p120d2, t_p30d2, t_syl250, t_syl30))	
		

F_dp.write('\n\n')
F_dp.write('P30d :today > yesterday +\n')		
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	if int(hycode) > 1000:
		continue
	F_dp.write('%s '%(DF.iat[i, md.BI_p120d2+1]))
	
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 50'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
		
	
	F_dp.write('p30d  ')	
	for k in range(50, -1, -1):		
		if df.iat[num-1-k, md.BI_p30d2] >  df.iat[num-1-k - 1, md.BI_p30d2]:
			tag_30 = '+'
		else:		
			tag_30 = '.'
		F_dp.write('%s'%tag_30)
	F_dp.write('[%6.0f]\n'%df.iat[num-1-k, md.BI_p30d2])	
		
######################################## dp_l.txt #######################
F_dp_l.write('code [p120d  p30d]  [syl250  syl30]\n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	if int(hycode) < 1000:
		F_dp_l.write('%4s%10s'%(DF.iat[i, md.BI_p120d2+1], str(DATE)[5:10]))
	
		if DF.iat[i, md.BI_p120d2] < 0:
			t_p120d2 = '-'
		else :	
			t_p120d2 = '+'
		
		if DF.iat[i, md.BI_p30d2] < 0:
			t_p30d2 = '-'
		else :	
			t_p30d2 = '+'
			
		if 	DF.iat[i, md.BI_syl250] < 0:
			t_syl250 = '-'
		elif DF.iat[i, md.BI_syl250] > 40:
			t_syl250 = '*'
		else:
			t_syl250 = '+'

		if 	DF.iat[i, md.BI_syl30] < 0:
			t_syl30 = '-'
		else:
			t_syl30 = '+'

		F_dp_l.write('  [%2s %2s]  [%2s %2s]\n'%(t_p120d2, t_p30d2, t_syl250, t_syl30))	
		

F_dp_l.write('\n\n')
F_dp_l.write('P30d :today > yesterday +\n')		
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	if int(hycode) > 1000:
		continue
	F_dp_l.write('%s '%(DF.iat[i, md.BI_p120d2+1]))
	
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 200'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
		
	
	F_dp_l.write('p30d  ')	
	for k in range(150, -1, -1):		
		if df.iat[num-1-k, md.BI_p30d2] >  df.iat[num-1-k - 1, md.BI_p30d2]:
			tag_30 = '+'
		else:		
			tag_30 = '.'
		F_dp_l.write('%s'%tag_30)
	F_dp_l.write('[%6.0f]\n'%df.iat[num-1-k, md.BI_p30d2])	

		
######################################## hy.txt #######################
F_hy.write('%s\n\n'%(str(DATE)[0:10]))
F_hy.write('syl30 < 0 .,else + or *\n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	hyname = DF.iat[i, md.BI_p120d2+2]
	if int(hycode) < 1000:
		continue
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 50'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
	if df.iat[num-1, md.BI_syl250] < 0:
		tag_250 = '-'
	elif df.iat[num-1, md.BI_syl250] > 40:
		tag_250 = '*'
	else:		
		tag_250 = '+'	
	F_hy.write('%6s'%hycode)
	F_hy.write('[%5.0f]'%df.iat[num-1, md.BI_p30d2])		
	F_hy.write('[%s]'%tag_250)		
		
	
	for k in range(30, -1, -1):		
		if df.iat[num-1-k, md.BI_syl30] < 0:
			tag_30 = '.'
		elif df.iat[num-1-k, md.BI_syl30] > 10:
			tag_30 = '*'
		else:		
			tag_30 = '+'
		F_hy.write('%s'%tag_30)
	F_hy.write('[%6.2f]'%df.iat[num-1-k, md.BI_syl30])	
	
	F_hy.write(' %s'%hyname)		
	F_hy.write('\n')		

F_hy.write('\n\np30d today > yestoday then + \n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	hyname = DF.iat[i, md.BI_p120d2+2]
	if int(hycode) < 1000:
		continue
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 50'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
	if df.iat[num-1, md.BI_syl250] < 0:
		tag_250 = '-'
	elif df.iat[num-1, md.BI_syl250] > 40:
		tag_250 = '*'
	else:		
		tag_250 = '+'	
	F_hy.write('%6s'%hycode)
	F_hy.write('[%5.0f]'%df.iat[num-1, md.BI_p30d2])		
	F_hy.write('[%s]'%tag_250)		


	for k in range(38, -1, -1):		
		if df.iat[num-1-k, md.BI_p30d2] >  df.iat[num-1-k - 1, md.BI_p30d2]:
			tag_30 = '+'
		else:		
			tag_30 = '.'
		F_hy.write('%s'%tag_30)
	F_hy.write(' %s'%hyname)		
	F_hy.write('\n')		
	
######################################## hy_l.txt #######################
F_hy_l.write('%s\n\n'%(str(DATE)[0:10]))
F_hy_l.write('syl30 < 0 .,else + or *\n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	hyname = DF.iat[i, md.BI_p120d2+2]
	if int(hycode) < 1000:
		continue
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 200'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
	if df.iat[num-1, md.BI_syl250] < 0:
		tag_250 = '-'
	elif df.iat[num-1, md.BI_syl250] > 40:
		tag_250 = '*'
	else:		
		tag_250 = '+'	
	F_hy_l.write('%6s'%hycode)
	F_hy_l.write('[%5.0f]'%df.iat[num-1, md.BI_p30d2])		
	F_hy_l.write('[%s]'%tag_250)		
		
	
	for k in range(150, -1, -1):		
		if df.iat[num-1-k, md.BI_syl30] < 0:
			tag_30 = '.'
		elif df.iat[num-1-k, md.BI_syl30] > 10:
			tag_30 = '*'
		else:		
			tag_30 = '+'
		F_hy_l.write('%s'%tag_30)
	F_hy_l.write('[%6.2f]'%df.iat[num-1-k, md.BI_syl30])	
	
	F_hy_l.write(' %s'%hyname)		
	F_hy_l.write('\n')		

F_hy_l.write('\n\np30d today > yestoday then + \n')
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	hyname = DF.iat[i, md.BI_p120d2+2]
	if int(hycode) < 1000:
		continue
	t_name = 'hyb' + hycode
	sql = 'SELECT * FROM ' +  t_name + ' ORDER BY date DESC  limit 200'
	df = pd.read_sql_query(sql, G_DBengine)
	del df['level_0']
	del df['index']
	df =  df.sort_values('date')
	me.PinghuaDF(df, md.BI_syl30, 5)
	me.PinghuaDF(df, md.BI_p120d2, 5)
	me.PinghuaDF(df, md.BI_p30d2, 5)
	me.PinghuaDF(df, md.BI_syl250, 30)
	
	num = df.index.size
	if df.iat[num-1, md.BI_syl250] < 0:
		tag_250 = '-'
	elif df.iat[num-1, md.BI_syl250] > 40:
		tag_250 = '*'
	else:		
		tag_250 = '+'	
	F_hy_l.write('%6s'%hycode)
	F_hy_l.write('[%5.0f]'%df.iat[num-1, md.BI_p30d2])		
	F_hy_l.write('[%s]'%tag_250)		


	for k in range(150, -1, -1):		
		if df.iat[num-1-k, md.BI_p30d2] >  df.iat[num-1-k - 1, md.BI_p30d2]:
			tag_30 = '+'
		else:		
			tag_30 = '.'
		F_hy_l.write('%s'%tag_30)
	F_hy_l.write(' %s'%hyname)		
	F_hy_l.write('\n')		

	
F_hy_l.write('\n\n')	


	
## out put num 5 fongsi for every hangye
for i in range(DF.index.size):
	hycode = DF.iat[i, md.BI_p120d2+1]
	hyname = DF.iat[i, md.BI_p120d2+2]
	if int(hycode) < 1000:
		continue
	print 'Output Top5 for %s %s........'%(hycode, hyname)	
	F_hy.write('%6s'%hycode)
	F_hy.write(' %s-------------------------\n'%hyname)
	num = 0
	sql = 'SELECT * FROM hy' + hycode 
	hy_gongsis = pd.read_sql_query(sql, G_DBengine) 
	for i in range(Qdpm.index.size):	
		gs = Qdpm.loc[i, 'code']
		gs_name = Qdpm.loc[i, 'name']
		#df1 = Index[Index.code == gs]
		df1 = hy_gongsis[hy_gongsis.code == gs]
		if df1.index.size == 0:
			continue
		else:
			F_hy.write('      %6s'%gs)
			F_hy.write('      %12s\n'%gs_name)
			num = num + 1
			if num == 5:
				break
	
F_hy.close()
F_dp.close()	
F_hy_l.close()
F_dp_l.close()	

if is_mail:
	me.SndEmail(subject='DP is OK.........', filename='..\\data\\dp.txt')	
	time.sleep(10)
	me.SndEmail(subject='HY is OK.........', filename='..\\data\\hy.txt')			