import tushare as ts


def Init_30mDataFile(gs, from_date):
	print gs
	code = gs 
	startdate = from_date
	filename_tu  = '../data/tmp.txt'

	if int(code) > 600000:
		filename  = '../data/sh' + code + '.txt'
	else:	
		filename  = '../data/sz' + code + '.txt'
		
	#get data
	df = ts.get_hist_data(code,start = startdate, ktype = '30') 
	df.to_csv(filename_tu,columns=['close','volume'])

	#change data
	f = open(filename_tu,'r')
	fw = open(filename, 'w')
	is_firstline = True
	LD = []
	for line in f:
		if is_firstline:
			is_firstline = False
			continue
		print line
		line = line.replace(',',' ')
		print line
		line = line.split()
		date = line[0]
		time = line[1]
		price = float(line[2])
		mount = float(line[3])
		
		money = price * mount / 100
		LD.append([date, time, price, money])	
		
	f.close()
	print LD

	l = len(LD)
	t_money = 0.0
	for i in range(l):	
		fw.write('%16s'%(LD[l-i-1][0]))
		fw.write('%16s'%(LD[l-i-1][1]))
		fw.write('%12.2f'%(LD[l-i-1][2]))
		
		if i == 0:
			t_money = LD[l-i-1][3]
		else:
			if LD[l-i-1][0] == LD[l-i][0]:  #same day
				t_money = t_money + LD[l-i-1][3]
			else:
				t_money = LD[l-i-1][3]
		
		fw.write('%12.2f'%(t_money))
		fw.write('\n')	

	fw.close()	
	
	
