import pickle 

player_vs_player_stat = pickle.load( open( "res.p", "rb" ) )

for key, value in player_vs_player_stat.items():
	print key

	for i in value:
		print i