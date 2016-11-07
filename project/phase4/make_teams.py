players = open("players.csv", "r").readlines()
players = [i.strip().split(',')[0:2] for i in players]

m = dict()

for tmp in players:
    if tmp[1] in m:
        m[tmp[1]].append(tmp[0])
    else:
        m[tmp[1]] = [tmp[0]]
        
    
for team in m.keys():
    with open("teams/"+team+"/players", "w") as f:
        f.write('\n'.join(m[team]))
