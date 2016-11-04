import pickle 

player_vs_player_stat = pickle.load( open( "res.p", "rb" ) )
batsman_clusters = pickle.load( open( "batsman_clusters.p", "rb" ) )
bowler_clusters = pickle.load( open( "bowler_clusters.p", "rb" ) )

for i in player_vs_player_stat :
	if i[-1] == " ":
		i = i[:-1]

	print i		

print '-----------------------------------------------------------------------'

for key, value in player_vs_player_stat.items():
	for i in value:
		if i[-1] == " ":
			i = i[:-1]

	print i		

'''	
for i,j in player_vs_player_stat :
	print type(j)
'''
pickle.dump(  player_vs_player_stat , open( "playerVSplayer_stat.p", "wb" ) )  	