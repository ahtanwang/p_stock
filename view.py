#-*- coding: UTF-8 -*- 
import pylab
import numpy as np
import pandas as pd    
import MySQLdb
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import tushare as ts
import mylib as me
import mydef as md

global G_DBengine
G_DBengine = create_engine('mysql://root:pass@127.0.0.1/mystock?charset=utf8')
global Codes
Codes = pd.read_csv("..\\data\\codename.txt", converters={'code':str})

com1 = ' '
com2 = ' '
com3 = ' '

LW = 8


def Pinghua(df, index, days):
	pass


def HY_isIN_HYL(hyname):
	for i in range(len(md.HYL)):
		if hyname == md.HYL[i][0]:
			return True
	return False

def ShowGS(com1, com2, com3):
	df2 = Codes[Codes['code'].isin([com1])]
	print df2

	if com2 == 'syl30':
		tname = 'b'+com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		if df.index.size > 250:
			df = df.drop(range(df.index.size - 250))
		me.PinghuaDF(df, md.BI_syl30+1, 5)
		plt.fill_between(df.index, df['syl30'], 0, where=df['syl30']>0,facecolor='red')
		plt.fill_between(df.index, df['syl30'], 0, where=df['syl30']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))
		
	elif com2 == 'syl250':
		tname = 'b'+com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return	
		df = pd.read_sql_table(tname,G_DBengine)
		me.PinghuaDF(df, md.BI_syl250+1, 30)
		plt.fill_between(df.index, df['syl250'], 0, where=df['syl250']>0,facecolor='red')
		plt.fill_between(df.index, df['syl250'], 0, where=df['syl250']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))

		
	elif com2 == 'hb':
		tname = 'f'+com1	
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return	
		df = pd.read_sql_table('f'+com1,G_DBengine)
		df[1:df.index.size][['sjsrhb','sjlrhb']].plot(kind='bar',color={'red','green'})
		df[1:df.index.size]['nhgdqyl'].plot(color='blue', secondary_y=True, linewidth = LW)
		plt.title(com1 + '  ' +  com2 + '  ' + str(df.loc[df.index.size-1,'year']) + '  ' + str(df.loc[df.index.size-1,'season'])  )

	elif com2 == 'sr':
		tname = 'f'+com1	
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return	
		df = pd.read_sql_table('f'+com1,G_DBengine)
		df[1:df.index.size]['sjsr'].plot(kind='bar',color='green')
		df[1:df.index.size]['sjlr'].plot(color='red', secondary_y=True, linewidth=LW)				
		plt.title(com1 + '  ' +  com2 + '  ' + str(df.loc[df.index.size-1,'year']) + '  ' + str(df.loc[df.index.size-1,'season'])  )		
		
	else:
		print '[Error] input error ...'
		return
		
	plt.show()
	plt.close()

def ShowHY(com1, com2, com3):
	if com2 == 'syl30':
		tname = 'hyb' + com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		if df.index.size > 250:
			df = df.drop(range(df.index.size - 250))
		me.PinghuaDF(df, md.BI_syl30+2, 5)	
		plt.fill_between(df.index, df['syl30'], 0, where=df['syl30']>0,facecolor='red')
		plt.fill_between(df.index, df['syl30'], 0, where=df['syl30']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))
		
	elif com2 == 'syl250':
		tname = 'hyb' + com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		me.PinghuaDF(df, md.BI_syl250+2, 30)	
		plt.fill_between(df.index, df['syl250'], 0, where=df['syl250']>0,facecolor='red')
		plt.fill_between(df.index, df['syl250'], 0, where=df['syl250']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))

	elif com2 == 'p120d':
		tname = 'hyb' + com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		me.PinghuaDF(df, md.BI_p120d2+2, 5)	
		plt.fill_between(df.index, df['p120d2'], 0, where=df['p120d2']>0,facecolor='red')
		plt.fill_between(df.index, df['p120d2'], 0, where=df['p120d2']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))
		
	elif com2 == 'p30d':
		tname = 'hyb' + com1
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		if df.index.size > 250:
			df = df.drop(range(df.index.size - 250))
		me.PinghuaDF(df, md.BI_p30d2+2, 5)	
		plt.fill_between(df.index, df['p30d2'], 0, where=df['p30d2']>0,facecolor='red')
		plt.fill_between(df.index, df['p30d2'], 0, where=df['p30d2']<=0,facecolor='green')
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['date'])))
	
	elif com2 == 'sr':
		tname = 'fhy'+com1	
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return	
		df = pd.read_sql_table(tname,G_DBengine)
		df[1:df.index.size]['sjsr'].plot(kind='bar',color='green')
		df[1:df.index.size]['sjlr'].plot(color='red', secondary_y=True, linewidth=LW)	
		plt.title(com1 + '  ' +  com2 + '  ' + str(df.loc[df.index.size-1,'year']) + '  ' + str(df.loc[df.index.size-1,'season'])  )		


	elif com2 == 'gbsr':	
		tname = 'fhy'+com1	
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return	
		df = pd.read_sql_table(tname,G_DBengine)
		df[1:df.index.size]['sjsrhb'].plot(kind='bar',color='green')
		df[1:df.index.size]['sjlrhb'].plot(color='red', secondary_y=True, linewidth=LW)		
		plt.title(com1 + '  ' +  com2 + '  ' + str(df.loc[df.index.size-1,'year']) + '  ' + str(df.loc[df.index.size-1,'season'])  )
		
		
	else:
		print '[Error] input error ...'
		return
		
	plt.show()
	plt.close()	
	
def ShowMoney(com1, com2, com3):
	if True:
		tname = 'money'
		if me.IsTableExist(tname, G_DBengine) == False:
			print 'No table ....%s'%tname
			return
		df = pd.read_sql_table(tname,G_DBengine)
		df = df.sort_values('num',  ascending = False)	
		me.PinghuaDF(df, 18, 5)	
		me.PinghuaDF(df, 19, 5)	
		me.PinghuaDF(df, 20, 5)	
		df[['fm2','fm1']].plot(linewidth=LW)		
		df['m1dm2'].plot(color='red', secondary_y=True, linewidth=LW*2)	
		plt.title(com1 + '  ' +  com2 + '  '  + str(max(df['month'])))
		ax=plt.gca()  
		ax.set_xticklabels([df.iat[119,1], df.iat[99,1], df.iat[79,1], df.iat[59,1], df.iat[39,1], df.iat[19,1]])

	plt.show()
	plt.close()	

def man():
	print '   gscode  hb/sr/syl30/syl250'
	print '   hycode gbsr/sr/p30d/p120d/syl30/syl250'
	print '   hy -- output all hy info'
	print '   m1'
	
while True:
	com1 = ' '
	com2 = ' '
	com3 = ' '
	print  '------------------------------------------------------------'
	print  'Please input command(q for exit) .......'
	line = raw_input()
	lst = line.split(' ')
	if len(lst) == 1:
		com1 = lst[0]
	elif len(lst) == 2:
		com1 = lst[0]
		com2 = lst[1]
	elif len(lst) > 2:
		com1 = lst[0]
		com2 = lst[1]
		com3 = lst[2]
	print 'Your input command is : ', com1, com2, com3		
		
	if com1 == 'q':
		break 
		
	pd1 =  Codes[Codes['code'].isin([com1])]
	if pd1.size != 0:		#code valid
		ShowGS(com1, com2, com3)
		continue
		
	elif HY_isIN_HYL(com1) == True:
		ShowHY(com1, com2, com3)
		continue
	
	elif com1 == 'hy':
		for i in md.HYL:
			print i
		continue	

	elif com1 == 'm1':
		ShowMoney(com1, com2, com3)
		continue	

		
	print '[Error] input error ...'
	man()
	
	
		

	
	
	