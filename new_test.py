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
#batsman, bowler, total, 0s, 1,2,3,4,5,6,7,balls,wickets
#	data.append({'Batsman':i[0],'Bowler':i[1],'Total':player_vs_player[i]['total'],
#'0s':player_vs_player[i]['runs'][0],
#'1s':player_vs_player[i]['runs'][1],
#'2s':player_vs_player[i]['runs'][2],
#'3s':player_vs_player[i]['runs'][3],
#'4s':player_vs_player[i]['runs'][4],'5s':player_vs_player[i]['runs'][5],
#'6s':player_vs_player[i]['runs'][6],'7s':player_vs_player[i]['runs'][7],'Balls':player_vs_player[i]['balls'],'Wickets':player_vs_player[i]['wickets']})


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
	ball_by_ball[line] = p


dicts = spark.parallelize([ball_by_ball])
dicts.flatMap(lambda x: x.items())
print(dicts.collect())