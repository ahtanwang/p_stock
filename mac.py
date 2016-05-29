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

df = ts.get_money_supply() 
df['m1dm2'] = 0.0
df['fm2'] = 0.0
df['fm1'] = 0.0
df['num'] = int(0)

df2 = df.head(12 * 15)
for i in range(df2.index.size):
	m2 = float(df2.loc[i,'m2'])
	m1 = float(df2.loc[i,'m1'])
	df2.loc[i,'m1dm2'] = float(m1 / m2)
	df2.loc[i,'fm2'] = float(df2.loc[i,'m2_yoy'])
	df2.loc[i,'fm1'] = float(df2.loc[i,'m1_yoy'])
	df2.loc[i,'num'] = i

df2.sort_values('num',  ascending = False)	
print df2.head()
	
df2.to_sql('money', G_DBengine, if_exists='replace')

