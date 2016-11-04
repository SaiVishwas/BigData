import pickle 

player_vs_player_stat = pickle.load( open( "playerVSplayer_stat.p", "rb" ) )
clusters_of_bat = pickle.load( open( "batsman_clusters.p", "rb" ) )
clusters_of_bowl = pickle.load( open( "bowler_clusters.p", "rb" ) )

#print type(player_vs_player_stat)

batsman_clusters = dict()
for i,j in clusters_of_bat.iteritems():
	for t in j:
		batsman_clusters[t] = i

bowler_clusters = dict()
for i,j in clusters_of_bowl.iteritems():
	for t in j:
		bowler_clusters[t] = i


#print batsman_clusters
#print bowler_clusters

inter_cluster_stats = [[0]*8 for i in range(8)]
'''
for i,j in clusters_of_bat.iteritems() :
	tmp = [0 for i in range(10)]
	for bowl_cluster in clusters_of_bowl:
		for bowler,stat in j.iteritems() :
			if bowler_clusters[bowler] == bowl_cluster :
				for n in range(0,8):
					tmp[n] = tmp[n] + stat[n]
				tmp[8] = stat[9]
				tmp[9] = stat[10]
		
	inter_cluster_stats[i][bowl_cluster] = tmp

print inter_cluster_stats
'''

for bat_cluster_no , list_of_cluster_players in clusters_of_bat.iteritems():
	for bowl_cluster_no in range(0,8):
		tmp = [0 for i in range(10)]
		for batsman_name in list_of_cluster_players:
			bat_stat = player_vs_player_stat[batsman_name]
			for bowler_name , bowl_stat in bat_stat.iteritems():
				if bowler_clusters[bowler_name] == bowl_cluster_no :
					for n in range(0,8):
						tmp[n] = tmp[n] + bowl_stat[n]
				tmp[8] = bowl_stat[9]
				#tmp[9] = bowl_stat[10]
		
		inter_cluster_stats[bat_cluster_no][bowl_cluster_no] = tmp

print inter_cluster_stats		



