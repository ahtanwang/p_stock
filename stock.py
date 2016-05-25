#-*- coding: UTF-8 -*- 

import os
import urllib
import string
import time
import chardet
from HTMLParser import HTMLParser
import g30dk as g30
import mydef as md

			
def sleep(sec):
    time.sleep(sec)
    return

def GetNowTime():
    return time.strftime("%Y-%m-%d  %H:%M:%S",time.localtime(time.time()))	
	
global G_isMail 
	

class MyStock(object):
	LD = []
	# 0 date
	# 1 time
	# 2 price
	# 3 total money 
	# 4 syl
	# 5 syl5
	# 6 K money
	
	Price_pre = 0.0
	Price_now = 0.0
	is_dataValid = True
	
	def __init__(self, code, from_date):
		self.code = code
		self.ReadDataFromFile(from_date)
		
		
	def Run(self):	
		self.GetDataFromSina()
		self.Compute()
		
		

	def ReadDataFromFile(self, from_date):
		self.LD = []
		filename ="..\\data\\"+self.code+".txt"
		
		if os.path.exists(filename) == False:
			code2 = self.code[2:8]
			g30.Init_30mDataFile(code2, from_date)

		pre_date = ' '	
		d_money = 0.0
		pre_money = 0.0
		pre_price = 0.0
		f = open(filename,'r')
		for line in f:
			line = line.split()
			date = line[0]
			time = line[1]
			price = float(line[2])
			money = float(line[3])
			
			if pre_price != 0:
				if price / pre_price > 1.12 or price / pre_price < 0.88:
					self.is_dataValid = False
					
			pre_price = price	
			
			if date == pre_date:
				d_money = money - pre_money
				pre_money = money
			else:
				d_money = money
				pre_money = money
				pre_date = date
				
			self.LD.append([date, time, price, money, 0.0, 0.0, d_money])
		f.close()	
		
		print GetNowTime() + '  ' + self.code + '[OK]  read data from file......'
		#for i in self.LD:
		#	print i

	def GetDataFromSina(self):
		url = "http://hq.sinajs.cn/list=" + self.code
		s = urllib.urlopen(url).read()
		print GetNowTime() + '  ' + self.code + '[OK]  read data from sina......' 
				
		s = s.split(',')
		len_s = len(s)
		date = s[len_s-3]
		date.strip()
		time = s[len_s-2]
		time.strip()
		price = float(s[3])							#close
		money = float(s[9]) / 10000
		len_LD = len(self.LD) 
		if money == 0:
			return 
			
		if len_LD == 0:						#no any data
			self.LD.append([date, time, price, money, 0.0, 0.0, 0.0])	
			self.Append2DataFile()
			
		else:
			self.Price_now = price
			if (time == self.LD[len_LD-1][1]):			#if the same time, then drop the data
				print GetNowTime() + '  ' + self.code + '[warning] get the same time data......'
				return
			elif (date == self.LD[len_LD-1][0]):  		# same today  
				kmoney = money - self.LD[len_LD-1][3]
				if kmoney == 0:
					return 
				self.Price_pre = self.LD[len_LD-1][2]	
				self.LD.append([date, time, price, money,0.0,0.0,kmoney])
				self.Append2DataFile()
			else:	# new day
				self.Price_pre = self.LD[len_LD-1][2]
				self.LD.append([date, time, price, money, 0.0, 0.0,money])
				self.Append2DataFile()
		
		if price / self.Price_pre > 1.12 or price / self.Price_pre < 0.88:	
			self.is_dataValid = False
				
		#print GetNowTime() + '  ' + self.code + '[OK]  read data from sina, after append......' 
		#for i in self.LD:
		#	print i
			
		
				
	def Append2DataFile(self):
		filename ="..\\data\\" + self.code +".txt"
		len_LD = len(self.LD) 
		f = open(filename,'a')
		f.write('%16s'%( self.LD[len_LD-1][0]))
		f.write('%16s'%( self.LD[len_LD-1][1]))
		f.write('%12.2f'%( self.LD[len_LD-1][2]))
		f.write('%12.2f'%( self.LD[len_LD-1][3]))
		f.write('\n')
		f.close()
		
				
	def Compute(self):
		if len(self.LD) < 35:
			return
		
		for i_to in range(35, len(self.LD)): #compute syl for 30 time item
			money = 0.0
			sy = 0.0
			syl = 0
			for j_from in range(0,30):
				sy = sy + (self.LD[i_to][2] /  self.LD[i_to-j_from][2] -1 ) * self.LD[i_to-j_from][6]
				money = money + self.LD[i_to-j_from][6]
			syl = sy / money * 100
			self.LD[i_to][4] = syl 
		
		#for i in self.LD:
		#	print i
			
		for i in range(10, len(self.LD)): #average for 5
			syl5 = self.LD[i][4] + self.LD[i-1][4] + self.LD[i-2][4] + self.LD[i-3][4] + self.LD[i-4][4]
			syl5 = syl5 / 5	
			self.LD[i][5] = syl5
			
		self.is_Compute = True
		self.CheckBuyOrSale()	
		
		#print 'Compute--------------------------------------------------' 		
		#for i in self.LD:
		#	print i
			

	def CheckBuyOrSale(self):	#check buy or sale and email to QQ mailbox
		isMail = False
		lc = []
			
		l_LD = len(self.LD)	
		for i in range(20):
			if self.LD[l_LD-20+i][5] >= 5.0:
				lc.append('-')
			elif self.LD[l_LD-20+i][5] <= -5.0:	
				lc.append('B')
			else:		
				lc.append('.')
	
		l_lc = len(lc)
		if lc[l_lc-1] != lc[l_lc-2]:
			isMail = True
	
		if self.is_dataValid == True:
			s = ''.join(lc)
			s1 = self.code
			s1 = s1 + ',%4.1f,'%self.LD[l_LD-1][5] 
			s = s1 + s +'#'
		else:
			s = self.code + '   Data error.....'
		
		print 'CheckBuyOrSale--------------------------------------------------' 		
		print s
		if isMail or G_isMail:
			self.SndEmail(subject=s)
			isMail = False
	
	
	def SndEmail(self,
					from_addr = 'flint.wb@163.com', 
                   password = 'Ahtan0919wy', 
                   to_addrs = ('1135420765@qq.com'), 
                   subject = 'TestRusult', 
                   content = 'None' 
                   ): 
		print 'Send email ' + subject		   
		try: 
			from smtplib import SMTP 
			from email.mime.text import MIMEText 

			email_client = SMTP(host = 'smtp.163.com') 
			email_client.login(from_addr, password) 

			#create msg 
			msg = MIMEText(content, _charset = 'utf-8') 
			msg['Subject'] = subject 
			email_client.sendmail(from_addr,to_addrs, msg.as_string()) 
			return True 

		except Exception,e: 
			print e 
			return False 
		finally: 
			email_client.quit() 

			
###########################################		
s1 = MyStock('sz002709', '2016-05-13')			
s2 = MyStock('sz002772', '2016-05-01')			
s3 = MyStock('sz300230', '2016-05-01')			

hour = 1
min = 1
is_run = False
G_isMail = False

while 1:

	try:

		hour = time.localtime(time.time()).tm_hour
		min = time.localtime(time.time()).tm_min
	
		print GetNowTime() + '##timer .........................',hour, min

		if hour == 10 and min == 5:
			is_run = True
			G_isMail = True
		if hour == 10 and min == 35:
			is_run = True
		if hour == 11 and min == 5:
			is_run = True
		if hour == 11 and min == 35:
			is_run = True
		if hour == 13 and min == 35:
			is_run = True
		if hour == 14 and min == 5:
			is_run = True
		if hour == 14 and min == 30:
			is_run = True
		if hour == 14 and min == 50:
			is_run = True
		if hour == 15 and min == 5:
			is_run = True
			G_isMail = True
			
		if is_run:
			is_run = False
			print GetNowTime() + 'run one time.........................'
			s1.Run()
			time.sleep(10)
			s2.Run()
			time.sleep(10)
			s3.Run()
			time.sleep(60)
			if G_isMail:
				G_isMail = False
			


	except ValueError, e:
		print 'ValueError:', e
	except ZeroDivisionError, e:
		print 'ZeroDivisionError:', e	

	finally:
		time.sleep(30)
			

