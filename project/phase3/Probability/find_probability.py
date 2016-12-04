import pickle 
import random
from collections import defaultdict
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

def load_from_hive(filename):
	sqlContext.sql("USE ipl")
	query = sqlContext.sql("SELECT * FROM " + filename)
	#a = sqlContext.sql("select * from batsman2")
	#x = a.map(lambda p: p.name).collect()
	query.show()

	rows = query.rdd.map(lambda p: str(p.name) + "," + str(p.cluster)).collect()
	
	clusters = defaultdict(list)

	for i in rows :
		#print i
		name = i.split(",")[0]
		cluster_number = int(i.split(",")[1])
		clusters[cluster_number].append(name)

	return clusters	


def player_to_cluster_mapping(clusters_of_players)		:
	player_to_cluster = dict()
	for i,j in clusters_of_players.iteritems():
		for t in j:
			player_to_cluster[t] = i
	return player_to_cluster		

def create_cluster_vs_cluster_stats(bowler_clusters) :

	inter_cluster_stats = [[0]*8 for i in range(8)]

	for bat_cluster_no , list_of_cluster_players in clusters_of_bat.iteritems():
		for bowl_cluster_no in range(0,8):
			tmp = [0 for i in range(11)]
			for batsman_name in list_of_cluster_players:
				if batsman_name in player_vs_player_stat :
					bat_stat = player_vs_player_stat[batsman_name]
					#print bat_stat
					for bowler_name , bowl_stat in bat_stat.iteritems():
						print bowl_stat
						if bowler_clusters[bowler_name] == bowl_cluster_no :
							for n in range(0,11):
								tmp[n] = tmp[n] + bowl_stat[n]
				
			inter_cluster_stats[bat_cluster_no][bowl_cluster_no] = tmp

	return inter_cluster_stats		

def get_probability_of_run(batsman_name , bowler_name , runs ,cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) :
	bat_cluster_no = batsman_to_cluster_mapping[batsman_name]
	bowl_cluster_no = bowler_to_cluster_mapping[bowler_name]
	stats = cluster_vs_cluster_stats[bat_cluster_no][bowl_cluster_no]

	balls = sum(stats[:8])
	if runs <= 6	:
		freq = stats[runs]
	elif runs > 6 :
		freq = stats[7]

	probability = freq/float(balls)
	
	return probability	

def get_class(cumulative_pdf_range , n):
	for i in range(0,len(cumulative_pdf_range)):
		if n < cumulative_pdf_range[i]:
			return i
def simulate(batsman_name , bowler_name , cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping ):
	pdf = []

	for i in range(0,8)	:
		pdf.append(get_probability_of_run(batsman_name, bowler_name , i ,cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) )
	print pdf
	#print sum(pdf)	

	cumulative_pdf = []
	cumulative_pdf.append(pdf[0])
	for i in range(1,len(pdf)):
		cumulative_pdf.append(cumulative_pdf[i-1] + pdf[i])
	#print cumulative_pdf

	cumulative_pdf_range = []
	for i in range(0,len(cumulative_pdf)):
		cumulative_pdf_range.append(int(cumulative_pdf[i]*100))
	#print cumulative_pdf_range
	
	rand_no = random.randint(0,99)

	prediction = get_class(cumulative_pdf_range , rand_no)

	return prediction



def predict_next(batsman_name , bowler_name , cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) :
	bat_cluster_no = batsman_to_cluster_mapping[batsman_name]
	bowl_cluster_no = bowler_to_cluster_mapping[bowler_name]
	stats = cluster_vs_cluster_stats[bat_cluster_no][bowl_cluster_no]

	simulate(stats , batsman_name , bowler_name , cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping)


if __name__ == "__main__":
	player_vs_player_stat = pickle.load( open( "playerVSplayer_stat.p", "rb" ) )
	#clusters_of_bat = pickle.load( open( "batsman_clusters.p", "rb" ) )
	#clusters_of_bowl = pickle.load( open( "bowler_clusters.p", "rb" ) )

	clusters_of_bat = load_from_hive("batsmen")
	clusters_of_bowl = load_from_hive("bowler")

	batsman_to_cluster_mapping = player_to_cluster_mapping(clusters_of_bat)
	bowler_to_cluster_mapping = player_to_cluster_mapping(clusters_of_bowl)
	#print bowler_to_cluster_mapping
	cluster_vs_cluster_stats = create_cluster_vs_cluster_stats(bowler_to_cluster_mapping)
	#print cluster_vs_cluster_stats
	#print (get_probability_of_run("Rohit Sharma" , "Sunil Narine" , 1 ,cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) )
	#print (simulate("Rohit Sharma" , "Sunil Narine" , cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) )
	a = dict()
	a['batmap'] = batsman_to_cluster_mapping
	a['bowlmap'] = bowler_to_cluster_mapping
	a['clustervscluster'] = cluster_vs_cluster_stats
	pickle.dump(a, open("../phase4/mapping.bin", "wb"))

