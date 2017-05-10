import pandas as pd
import json, sys
import numpy as np
import itertools
import threading
import time
def myfunc(row):
    try: 
        return row['dtimestamp'].seconds 
    except: 
        return 0
def calculate(x):
    total=0
    valid=0
    valid_flag=False
    avg_valid=0
    tmp_count=0
    added=False
    vadded=False
    for i in range(len(x)):
        if str(x.iloc[i]['event']) == "ggstop":
            if i==0:
                continue
            if not added:
                total += 1
                added=True
            if valid_flag and not vadded:
                valid += 1    
                vadded=True
            if valid_flag:
                avg_valid = tmp_count
                if x.iloc[i]['dtimestamp'] > 30:
                    tmp_count =0
                    added=False
                    valid_flag=False
        elif str(x.iloc[i]['event']) == "ggstart":

            if x.iloc[i]['dtimestamp'] <= 0:
                 continue
            tmp_count += x.iloc[i]['dtimestamp']
            if tmp_count > 60:
                valid_flag=True 
    if valid > 0:
        avg=avg_valid/valid
    else:
        avg=0
    boo={'total':total,'valid':valid,'Avg_valid': avg}
    return boo


def readData():
	df=pd.read_json(sys.argv[1], lines=True)
	tmp={}
	og={}
	for i in range(len(df)):
	    og={}
	    for j in range(4):
	        if "debug" and "random" and "sdkv" in df.ix[i][j]:
	            og['ai5']=(df.ix[i][j]['ai5'])
	            continue
	        og.update(df.ix[i][j])
	    tmp[i]=og
	tmp
	return pd.DataFrame.from_dict(tmp, orient='index')
def main():
	df=readData()
	g_id=[]
	for i in df.game_id.unique():
	    g_id.append(int(i))
	a_id=[]
	for i in df.ai5.unique():
	    a_id.append(str(i))

	df1=df.groupby('game_id')
	report={}
	tmp=[]
	g_count=0
	a_count=0
	for i in g_id:
	    tmp=[]
	    g_count +=1
	    g_gid=df1.get_group(unicode(i))
	    g_ai5=g_gid.groupby('ai5')
	    g_ai5.ai5.unique
	    for j in g_ai5.ai5.unique():
	        a_count +=1
	        a=g_ai5.get_group(j[0]).sort_values(by="timestamp")
	        a=a.filter(['event','timestamp','ts'],axis=1)
	        a['ts'] = a.apply(lambda row : int(row['ts']), axis=1)
	        a['dts']= (a['ts'] - a['ts'].shift(-1))/1000.0
	        a['timestamp'] = a.apply(lambda row : pd.to_datetime(row['timestamp']), axis=1)
	        a['dtimestamp']= pd.to_timedelta(a['timestamp'].shift(-1) - a['timestamp'])
	        a['dtimestamp'] = a.apply(myfunc, axis=1)
	        tmp.append(calculate(a))
	    tmp1=reduce(lambda x, y: dict((k, v + y[k]) for k, v in x.iteritems()), tmp)
	    report[i]=tmp1
	user_stats=[]
	for i in g_id:
	    g1=(df.groupby('game_id')).get_group(unicode(i))
	    user=str(g1.ai5.describe()['top'])
	    freq=int(g1.ai5.describe()['freq'])
	    a={'gameid':i,'user':user,'freq':freq}
	    user_stats.append(a)
	pd.DataFrame.from_dict(user_stats).sort_values(by='gameid').to_csv('user_stats.csv',sep='\t', encoding='utf-8', index=False)
	report_df=pd.DataFrame.from_dict(report, orient="index")
	report_df.to_csv("Report.csv", sep='\t', encoding='utf-8')
	print report_df.sum()

def start():
	done = False
	def animate():
	    for c in itertools.cycle(['|', '/', '-', '\\']):
		if done:
		    break
		sys.stdout.write('\rloading ' + c)
		sys.stdout.flush()
		time.sleep(0.1)
	    sys.stdout.write('\rDone!     \n')

	t = threading.Thread(target=animate)
	t.start()

	main()
	time.sleep(10)
	done = True
start()
