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
	ball_by_ball[line] = p,details
	if bowler in yy.keys():
		yy[bowler][0]+=int(details['balls'])
		yy[bowler][1]+=int(details['total'])
	else:
		yy[bowler]=[int(details['balls']),int(details['total'])]
	if batsmen in y.keys():
		y[batsmen][0]+=int(details['total'])
		y[batsmen][1]+=int(details['balls'])
	else:
		y[batsmen]=[int(details['total']),int(details['balls'])]

del_bowlers = []
del_batsman = [] #to delete useless bowlers and batsmen
for k in yy.keys():
	if(yy[k][1]!=0):
		yy[k] = float(yy[k][0]/yy[k][1]) * 6
	else:
		del_bowlers.append(k)
for k in y.keys():
	if(y[k][1]!=0):
		y[k] = float(y[k][0]/y[k][1]) * 100
	else:
		del_batsman.append(k)
for i in del_bowlers:
	del yy[i]
for i in del_batsman:
	del y[i]
a = list(yy.items())
a.sort(key=lambda x: x[1],reverse=True)
b = list(y.items())
b.sort(key=lambda x: x[1],reverse=True)
classifier = lambda lst, sz: [lst[i:i+int(len(lst)/sz)] for i in range(0, len(lst), int(len(lst)/sz))]
a = classifier(a,10)
b = classifier(b,10)
def fuck(l):
	a = []
	for i in l:
		b = []
		for j in i:
			b.append(j[0])
		a.append(b)
	return a
a = fuck(a)
b = fuck(b)
rdd_bowler = spark.parallelize(a)
rdd_batsman = spark.parallelize(b)
rdd_batsman.saveAsPickleFile("batsman_cluster", 3)
rdd_bowler.saveAsPickleFile("bowler_cluster", 3)

# cluster vs cluster
# go through each combination of cluster
# find pairs that exist
# cumulate their data
# calculate probabilities

# new_cluster={}

# for i in range(10):
# 	for j in range(len(b[i])):
# 		for k in range(10):
# 			clusterdata=[0 for x in range(10)]
# 			for l in range(len(a[i])):
# 				players=(b[i][j],a[k][l])
# 				if players in ball_by_ball:
# 					clusterdata[0] += int(ball_by_ball[players][1]['runs'][0])
# 					clusterdata[1] += int(ball_by_ball[players][1]['runs'][1])
# 					clusterdata[2] += int(ball_by_ball[players][1]['runs'][2])
# 					clusterdata[3] += int(ball_by_ball[players][1]['runs'][3])
# 					clusterdata[4] += int(ball_by_ball[players][1]['runs'][4])
# 					clusterdata[5] += int(ball_by_ball[players][1]['runs'][5])
# 					clusterdata[6] += int(ball_by_ball[players][1]['runs'][6])
# 					clusterdata[7] += int(ball_by_ball[players][1]['runs'][7])
# 					clusterdata[8] += int(ball_by_ball[players][1]['balls'])
# 					clusterdata[9] += int(ball_by_ball[players][1]['wickets'])
# 			s=str(i)+":"+str(k)
# 			new_cluster[s]=clusterdata

# #print(new_cluster)
# for k,v in new_cluster.items():
# 	p = [(i+1)*1/9 for i in range(9)]
# 	if int(v[8])!=0:
# 		p[0] = float(int(v[0])/int(v[8]))
# 		p[1] = p[0] + float(int(v[1])/int(v[8]))
# 		p[2] = p[1] + float(int(v[2])/int(v[8]))
# 		p[3] = p[2] + float(int(v[3])/int(v[8]))
# 		p[4] = p[3] + float(int(v[4])/int(v[8]))
# 		p[5] = p[4] + float(int(v[5])/int(v[8]))
# 		p[6] = p[5] + float(int(v[6])/int(v[8]))
# 		p[7] = p[6] + float(int(v[7])/int(v[8]))
# 		p[8] = p[7] + float(int(v[9])/int(v[8]))
# 	new_cluster[k]=p





# schemaString = "pair 0 1 2 3 4 5 6 7 out"
# fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
# schema = StructType(fields)
# aa = ss(sparkContext = spark)
# df = []
# cl_df = []
# for k,v in ball_by_ball.items():
# 	df.append([k[0]+":"+k[1],v[0][0],v[0][1],v[0][2],v[0][3],v[0][4],v[0][5],v[0][6],v[0][7],v[0][8]])
# for k,v in new_cluster.items():
# 	cl_df.append([k,v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7],v[8]])
# b = aa.createDataFrame(data = df,schema = schema)
# bb = aa.createDataFrame(data = cl_df, schema = schema)
# bb.write.save("cvc.parquet")
# b.write.save("pvp.parquet")













spark.stop()