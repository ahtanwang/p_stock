#-*- coding: UTF-8 -*- 
import os, smtplib, mimetypes  
from email.mime.text import MIMEText  
from email.mime.image import MIMEImage  
from email.mime.multipart import MIMEMultipart  
import time
import tushare as ts
from sqlalchemy import create_engine
import pandas as pd
import pylab
import MySQLdb
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import tushare as ts

def GetNowTime():
    return time.strftime("%Y-%m-%d  %H:%M:%S  ",time.localtime(time.time()))	
	
def IsTableExist(tablename, G_DBengine):
	sql =  'select * from information_schema.tables where table_name = ' + '\'' + tablename + '\''
	df = pd.read_sql_query(sql, G_DBengine)
	if df.size == 0:    #table is not exist
		return False
	return True
	
def Is_dateInTable(tablename, G_DBengine, date2):	
	sql = 'select * from ' + tablename + ' where date = '+ '\'' + str(date2) + '\''
	df = pd.read_sql_query(sql, G_DBengine)
	if df.size == 0:  
		return False
	return True

def GetLatestDateFromTable(tablename, G_DBengine):
	if IsTableExist(tablename, G_DBengine) == False:
		return '1000-01-01'
	sql = 'select max(date) from ' +  tablename
	df = pd.read_sql_query(sql, G_DBengine)
	t_str = str(df.iat[0,0])
	date = t_str[0:10]
	return date

		
def GetNumFromTable(tablename, G_DBengine):
	if IsTableExist(tablename, G_DBengine) == False:
		return 0
		
	sql = 'select count(*) from ' +  tablename
	df = pd.read_sql_query(sql, G_DBengine)
	return df.iat[0,0]		
	
def MyDiv(a, b):
	if b == 0:
		return 0.0
	else:		
		return float(a/b)
		
def PinghuaDF(df, index, days):
	for i in range(df.index.size-1, days, -1): 
		total = 0.0
		for j in range(i-days+1, i+1):
			total = total + df.iat[j,index]
		total = total / days
		df.iat[i,index] = total


 
def SndEmail(subject, content = 'notice for stock..', filename = None):  
    MAIL_LIST = ["1135420765@qq.com"]  
    MAIL_HOST = "smtp.163.com" 

    df1=pd.read_csv('..\\config\\email_a.txt')
    MAIL_USER = df1.loc[0,'send']
    MAIL_PASS = df1.loc[0,'pass']
    MAIL_FROM = df1.loc[0,'from']

    try:  
        message = MIMEMultipart()  
        message.attach(MIMEText(content))  
        message["Subject"] = subject  
        message["From"] = MAIL_FROM  
        message["To"] = ";".join(MAIL_LIST)  
        if filename != None and os.path.exists(filename):  
            ctype, encoding = mimetypes.guess_type(filename)  
            if ctype is None or encoding is not None:  
                ctype = "application/octet-stream" 
            maintype, subtype = ctype.split("/", 1)  
            attachment = MIMEImage((lambda f: (f.read(), f.close()))(open(filename, "rb"))[0], _subtype = subtype)  
            attachment.add_header("Content-Disposition", "attachment", filename = filename)  
            message.attach(attachment)  
 
        smtp = smtplib.SMTP()  
        smtp.connect(MAIL_HOST)  
        smtp.login(MAIL_USER, MAIL_PASS)  
        smtp.sendmail(MAIL_FROM, MAIL_LIST, message.as_string())  
        smtp.quit()  
		
        print 'SndEmail success....' + subject
        return True 
    except Exception, errmsg:  
        print "Send mail failed to: %s" % errmsg  
        return False 

		