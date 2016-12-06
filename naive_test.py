import findspark
findspark.init()
import time
import numpy
import random
from pyspark import SparkContext as sc
from pyspark import SparkConf
from pyspark.sql import SparkSession as ss
from pyspark.sql.types import *
conf = SparkConf()
conf.setMaster("spark://Sarthaks-MacBook-Pro.local:7077").setAppName('IPL Analytics Job').set("spark.executor.memory", "512m")
spark = sc(conf=conf)
spark_session = ss(spark)
a = spark.textFile("data.txt").map(lambda line : line.split("\n")[0]).collect()
df = spark_session.read.load("pvp.parquet")
df.createOrReplaceTempView("pvp")
sqlDF = spark_session.sql("SELECT * FROM pvp").rdd.map(lambda r : r).collect()
pairs = {n[0]: n[1:] for n in sqlDF}
batting = a[:11]
bowling = a[11:]
strike = batting.pop(0)
non_strike = batting.pop(0)
uniform_distribution = { i : (i+1)*1/9 for i in range(9)}

alpha = 0.95
cluster_vs_cluster = {}
def redistribute(pair, event):
	if pair in pairs:
		pd = pairs[pair]
		pd2 = []
		pd2.append(float(pd[0]))
		pd3 = []
		for i in range(1,len(pd)):
			pd2.append(float(pd[i]) - float(pd[i-1]))
		#print(pd)
		#print(pd2)
		temp = pd2[event]
		pd2[event] = temp * alpha
		for i in range(len(pd2)):
			pd2[i] = pd2[i] + (temp * (1 - alpha)/9)
		pd3 = pd2
		for i in range(1,len(pd2)):
			pd3[i] = (float(pd3[i]) + float(pd3[i-1]))
		pairs[pair] = pd3
		#print(pd3)
	return event

def predict(pair,ball):
	p = {}
	r_no = balls[ball-1]
	if pair in ball_by_ball:
		p = ball_by_ball[pair][0]
	else:
		batsman,bowler = pair.split(":")
		p = cvc(pair)
	#p = cvc(pair)
	if r_no < p[0]:
		return 0
	if r_no < p[1]:
		return 1
	if r_no < p[2]:
		return 2
	if r_no < p[3]:
		return 3
	if r_no < p[4]:
		return 4
	if r_no < p[5]:
		return 5
	if r_no < p[6]:
		return 6
	if r_no < p[7]:
		return 7
	if r_no < p[8]:
		return -1
	return 0
def naive_prediction(pair,ball):
	p = {}
	r_no = balls[ball-1]
	if pair in pairs:
		pd = pairs[pair]
		p[0] = float(pd[0])
		p[1] = float(pd[1])
		p[2] = float(pd[2])
		p[3] = float(pd[3])
		p[4] = float(pd[4])
		p[5] = float(pd[5])
		p[6] = float(pd[6])
		p[7] = float(pd[7])
		p[8] = float(pd[8])
	else:
		p = uniform_distribution
	if r_no < p[0]:
		return redistribute(pair,0) 
	if r_no < p[1]:
		return redistribute(pair,1) 
	if r_no < p[2]:
		return redistribute(pair,2) 
	if r_no < p[3]:
		return redistribute(pair,3) 
	if r_no < p[4]:
		return redistribute(pair,4) 
	if r_no < p[5]:
		return redistribute(pair,5) 
	if r_no < p[6]:
		return redistribute(pair,6) 
	if r_no < p[7]:
		return redistribute(pair,7) 
	if r_no < p[8]:
		return -1
	return 0
def swap(s,ns):
	a = s
	s = ns
	ns = s
	return s,ns
balls_team_1 = 0
wickets_team_1 = 0 
balls_team_2 = 0
wickets_team_2 = 0 
score_team_1 = 0
score_team_2 = 0
ball_by_ball = {}
bowler_cluster = []
batsman_cluster = []
balls = [random.random() for i in range(150)]
random.shuffle(balls)
def start_sim(batting,bowling,strike,non_strike,balls_team_1,score_team_1,wickets_team_1):
	for bowler in bowling:
		#bb = 0 
		for i in range(6):
			#bb+=1
			balls_team_1+=1
			outcome = naive_prediction(strike+":"+bowler,balls_team_1)
			print(outcome)
			if outcome == 1 or outcome == 3:
				strike,non_strike = swap(strike,non_strike)
				score_team_1+=outcome
			elif outcome == -1:
				wickets_team_1+=1
				if wickets_team_1==10:
					return([score_team_1,wickets_team_1,balls_team_1])
				else:
					strike = batting.pop(0)
			elif outcome == 5 or outcome==7:
				#balls_team_1-=1
				#bb-=1
				score_team_1+=(outcome + int(random.random()*7))
			else:
				score_team_1 += outcome
		strike,non_strike = swap(strike,non_strike)
	return([score_team_1,wickets_team_1,balls_team_1])

print(start_sim(batting,bowling,strike,non_strike,balls_team_1,score_team_1,wickets_team_1))
spark.stop()

