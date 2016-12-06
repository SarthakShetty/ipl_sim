import findspark
findspark.init()
import time
from pyspark import SparkContext as sc
from pyspark import SparkConf
from pyspark.sql import SparkSession as ss
from pyspark.sql.types import *
conf = SparkConf()
conf.setMaster("spark://Sarthaks-MBP:7077").setAppName('IPL Analytics Job').set("spark.executor.memory", "512m")
spark = sc(conf=conf) 
a = spark.textFile("Dataset/*.csv").map(lambda line: line.split(",")).filter(lambda line: line[0].strip()=="ball").collect()
player_vs_player={}
for line in a:
	details = line
	if details[0].strip()=='ball':
		players1=(details[4],details[6])
		players2=(details[5],details[6])

		if players1 in player_vs_player.keys():
			player_vs_player[players1]['total']+=int(details[7])
			player_vs_player[players1]['runs'][int(details[7])]+=1
			player_vs_player[players1]['balls']+=1
			if details[9]!='""' and details[9]!='run out' and players1[0].strip()==details[10].strip():
				player_vs_player[players1]['wickets']+=1
		else:
			player_vs_player[players1]={}
			player_vs_player[players1]['total']=int(details[7])
			player_vs_player[players1]['runs']=[0,0,0,0,0,0,0,0]
			player_vs_player[players1]['wickets']=0
			player_vs_player[players1]['balls']=1
			if details[9]!='""' and details[9]!='run out' and players1[0].strip()==details[10].strip():
				player_vs_player[players1]['wickets']+=1

		if players2 in player_vs_player.keys():
			player_vs_player[players2]['total']+=int(details[8])
			player_vs_player[players2]['runs'][int(details[8])]+=1
			player_vs_player[players2]['balls']+=1
			if details[9]!='""' and details[9]!='run out' and players2[0].strip()==details[10].strip():
				player_vs_player[players2]['wickets']+=1
		else:
			player_vs_player[players2]={}
			player_vs_player[players2]['total']=int(details[8])
			player_vs_player[players2]['runs']=[0,0,0,0,0,0,0,0]
			player_vs_player[players2]['wickets']=0
			player_vs_player[players2]['balls']=1
			if details[9]!='""' and details[9]!='run out' and players2[0].strip()==details[10].strip():
				player_vs_player[players2]['wickets']+=1
y={}
yy={}
ball_by_ball = {}
for line in player_vs_player.keys():
	details = line
	batsmen = details[0]
	bowler = details[1]
	details = player_vs_player[line]
	p = {}
	p[0] = float(int(details['runs'][0])/int(details['balls']))
	p[1] = p[0] + float(int(details['runs'][1])/int(details['balls']))
	p[2] = p[1] + float(int(details['runs'][2])/int(details['balls']))
	p[3] = p[2] + float(int(details['runs'][3])/int(details['balls']))
	p[4] = p[3] + float(int(details['runs'][4])/int(details['balls']))
	p[5] = p[4] + float(int(details['runs'][5])/int(details['balls']))
	p[6] = p[5] + float(int(details['runs'][6])/int(details['balls']))
	p[7] = p[6] + float(int(details['runs'][7])/int(details['balls']))
	p[8] = p[7] + float(int(details['wickets'])/int(details['balls']))
	ball_by_ball[line] = p,details
	if bowler in yy.keys():
		yy[bowler][0]+=int(details['balls'])
		yy[bowler][1]+=int(details['total'])
	else:
		yy[bowler]=[int(details['balls']),int(details['total']),int(details['wickets'])]
	if batsmen in y.keys():
		y[batsmen][0]+=int(details['total'])
		y[batsmen][1]+=int(details['balls'])
		y[batsmen][2]+=int(details['runs'][6])
	else:
		y[batsmen]=[int(details['total']),int(details['balls']),int(details['runs'][6])]

del_bowlers = []
del_batsman = [] #to delete useless bowlers and batsmen
for k in yy.keys():
	if(yy[k][1]!=0):
		yy[k] = float(yy[k][0]/yy[k][1]) * 6 , yy[k][2]
	else:
		del_bowlers.append(k)
for k in y.keys():
	if(y[k][1]!=0):
		y[k] = float(y[k][0]/y[k][1]) * 100 , y[k][2]
	else:
		del_batsman.append(k)
for i in del_bowlers:
	del yy[i]
for i in del_batsman:
	del y[i]

rdd_bowler = spark.parallelize(yy)
rdd_batsman = spark.parallelize(y)
all_batsmen = spark.parallelize([y])
all_bowlers = spark.parallelize([yy])
all_bowlers.saveAsPickleFile("bowlers", 3)
all_batsmen.saveAsPickleFile("batsmen", 3)


spark.stop()