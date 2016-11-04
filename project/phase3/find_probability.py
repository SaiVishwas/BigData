import pickle 

player_vs_player_stat = pickle.load( open( "playerVSplayer_stat.p", "rb" ) )
batsman_clusters = pickle.load( open( "batsman_clusters.p", "rb" ) )
bowler_clusters = pickle.load( open( "bowler_clusters.p", "rb" ) )

#print type(bowl_clusters)

list_of_players = []

for i in batsman_clusters :
	for j in batsman_clusters[i]:
		list_of_players.append(j)

list_of_players = sorted(list_of_players)		

for i in list_of_players:
	print i
'''
print len(player_vs_player_stat)

to_be_removed = set()
for i in player_vs_player_stat :
	if i[-1] == ' ':
		i = i[:-1]
	if i not in list_of_players:
		#del player_vs_player_stat[i]
		to_be_removed.add(i)
		#print i

print to_be_removed

for i in to_be_removed :
	if i in player_vs_player_stat:
		print i
		del player_vs_player_stat[i]


print player_vs_player_stat

for i in player_vs_player_stat:
	print type(i)
'''
'''
	for j in to_be_removed:
		if j in i:
			del i[j]
'''			


'''
for i in list_of_players:
	print i 

#player_vs_player_stat = {k:v for k,v in player_vs_player_stat.iteritems() if k }
'''
'''
print len(player_vs_player_stat)		
'''


