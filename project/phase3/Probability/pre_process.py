import pickle 

#res-initial.p is the pickle file obtained by dumping output player vs player stats generted
#however there is nae mismatch eg Rohit Sharma ( in clusters ) and RG Sharma (in player vs player)
# after correcting this mistake we get res.p 

player_vs_player_stat = pickle.load( open( "res.p", "rb" ) )
batsman_clusters = pickle.load( open( "batsman_clusters.p", "rb" ) )
bowler_clusters = pickle.load( open( "bowler_clusters.p", "rb" ) )

# list of valid players 

list_of_players = []

for i in batsman_clusters :
	for j in batsman_clusters[i]:
		list_of_players.append(j)

#remove entries of players not considered in playing 88
#print (player_vs_player_stat)
'''
valid_player_vs_player_stat = {k:v for k,v in player_vs_player_stat.iteritems() if k in list_of_players }

for key,val in valid_player_vs_player_stat.iteritems():
	{k:v for k,v in val.iteritems() if k in list_of_players }
'''

valid_player_vs_player_stat = dict()

for i,j in player_vs_player_stat.iteritems():
	tmp = dict()
	if i in list_of_players:
		for k,v in j.iteritems():
			if k in list_of_players:
				tmp[k] = v
		valid_player_vs_player_stat[i] = tmp




fin_players = set()
for i,j in valid_player_vs_player_stat.iteritems() :
	fin_players.add(i)
	for x in j:
		fin_players.add(x)

#pfor i in sorted(fin_players):
	#if i not in list_of_players:
	print i

pickle.dump( valid_player_vs_player_stat , open( "playerVSplayer_stat.p", "wb" ) )  	