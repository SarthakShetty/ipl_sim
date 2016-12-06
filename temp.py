import findspark
findspark.init()
import time
import random
import numpy as np
from pyspark import SparkContext as sc
from pyspark import SparkConf
from pyspark.sql import SparkSession as ss
from pyspark.mllib.clustering import KMeansModel
conf = SparkConf()
conf.setMaster("spark://Sarthaks-MBP:7077").setAppName('IPL Analytics Job').set("spark.executor.memory", "512m")
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
all_batsmen = spark.pickleFile("batsmen", 5).flatMap(lambda x:x.items())
all_bowlers = spark.pickleFile("bowlers", 5).flatMap(lambda x:x.items())

batsmen_points = all_batsmen.map(lambda x : x[1])
bowler_points = all_bowlers.map(lambda x : x[1])

k = 10
# def format_input(rdd):
# 	data = []
# 	size = rdd.count()
# 	for i in rdd.collect():
# 		data.append(i[0])
# 		data.append(i[1])
# 	return np.array(data).reshape(size, 2)

# def cluster(data):
#   model = KMeans.train(spark.parallelize(data), k, maxIterations=100, initializationMode="random",seed=50, initializationSteps=5, epsilon=1e-4)
#   cluster_centers = model.clusterCenters
#   return cluster_centers,model

# batsmen_data = format_input(batsmen_points)
# bowlers_data = format_input(bowler_points)


# batsmen_centres,batsmen_model = cluster(batsmen_data)
# bowlers_centres,bowlers_model = cluster(bowlers_data)

batsmen_model = KMeansModel.load(spark, "batsmen_model")
bowlers_model = KMeansModel.load(spark, "bowlers_model")
bat_dict = all_batsmen.collectAsMap()
bowl_dict = all_bowlers.collectAsMap()


#similar_batsmen = all_batsmen.map(lambda x : batsmen_model.predict(x[1]) == batsmen_model.predict(all_batsmen.collect()["batsman"]))

# bowler_cluster = spark.pickleFile("bowler_cluster", 5).map(str).collect()
# batsman_cluster = spark.pickleFile("batsman_cluster", 5).map(str).collect()

# all_batsmen = spark.pickleFile("batsmen", 5).flatMap(lambda x:x.items())
# all_bowlers = spark.pickleFile("bowlers", 5).flatMap(lambda x:x.items())


k = 10
def format_input(rdd):
	data = []
	size = rdd.count()
	for i in rdd.collect():
		data.append(i[0])
		data.append(i[1])
	return np.array(data).reshape(size, 2)

# batsmen_model = KMeansModel.load(spark, "batsmen_model")
# bowlers_model = KMeansModel.load(spark, "bowlers_model")

def cvc(pair):
	batsman,bowler = pair.split(":")
	decision = random.randrange(0,2)
	if decision == 1:
		similar_bowlers = all_bowlers.filter(lambda x : bowlers_model.predict(x[1]) == bowlers_model.predict(bowl_dict[bowler])).collect()
		for new_bowler in similar_bowlers:
			new_pair = batsman+":"+new_bowler[0]
			return cvc_p(new_pair)
	else:
		similar_batsmen = all_batsmen.filter(lambda x : batsmen_model.predict(x[1]) == batsmen_model.predict(bat_dict[batsman])).collect()
		for new_batsman in similar_batsmen:
			new_pair = new_batsman[0]+":"+bowler
			return cvc_p(new_pair)

def cvc_p(pair):
	p = {}
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
		p = cvc(pair)
	return p

def clustered_prediction(pair,ball):
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
		p = cvc(pair)
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

balls = [random.normalvariate(0.7,0.1) for i in range(150)]
#balls = [random.random() for i in range(150)]
random.shuffle(balls)

def start_sim(batting,bowling,strike,non_strike,balls_team_1,score_team_1,wickets_team_1):
	for bowler in bowling:
		#bb = 0 
		for i in range(6):
			#bb+=1
			balls_team_1+=1
			outcome = clustered_prediction(strike+":"+bowler,balls_team_1)
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
