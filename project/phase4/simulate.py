import pickle
import random

class Batsmen:
    def __init__(self, name, id, out_prob):
        self.name = name
        self.id = int(id)
        self.out_prob = out_prob
        self.tot_prob = 1
        self.runs = 0
        self.balls = 0
        
    def get_name(self):
        return self.name
        
    def get_id(self):
        return self.id
        
    def add_runs(self, x):
        self.runs += x
        
    def is_out(self):
        return self.tot_prob < 0.5
            
    def update_prob(self):
        self.tot_prob *= self.out_prob
        
    def increment_balls(self):
        self.balls += 1
        
    def __str__(self):
        return "{:50}{:3d} ({})".format(self.name, self.runs, str(self.balls))
        
class Bowler:
    def __init__(self, name, id):
        self.name = name
        self.id = int(id)
        self.runs = 0
        self.overs = 0
        self.wickets = 0
        self.part = None
        
    def get_name(self):
        return self.name
        
    def get_id(self):
        return self.id
        
    def add_runs(self, x):
        self.runs += x
        
    def get_overs(self):
        return self.overs
        
    def increment_overs(self):
        self.overs += 1
        
    def increment_wickets(self):
        self.wickets += 1
        
    def set_part(self, part):
        self.part = part
        
    def __str__(self):
        econ = "-"
        overs = str(self.overs)
        if self.overs:
            econ = str("{:.2f}".format(self.runs / (self.overs)))
        if self.part:
            overs = str(self.overs)+"."+str(self.part)
            econ = str("{:.2f}".format(self.runs / (self.overs + self.part/6)))
        return "{:30}{:20}{:20}{:20}{:20}".format(self.name, overs, str(self.wickets), str(self.runs), econ)
        
        
def get_probability_of_run(batsman_name , bowler_name , runs ,cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) :
	bat_cluster_no = batsman_to_cluster_mapping[batsman_name]
	bowl_cluster_no = bowler_to_cluster_mapping[bowler_name]
	stats = cluster_vs_cluster_stats[bat_cluster_no][bowl_cluster_no]
	balls = max(sum(stats[:8]), 1)
	if runs <= 6	:
		freq = stats[runs]
	elif runs > 6 :
		freq = stats[7]

	probability = freq/float(balls)
	return probability	

def get_class(cumulative_pdf_range , n):
	for i in range(len(cumulative_pdf_range)):
		if n <= cumulative_pdf_range[i]:
			return i
			
def simulate_ball(batsman_name , bowler_name , cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping ):
	pdf = []
	for i in range(8):
		pdf.append(get_probability_of_run(batsman_name, bowler_name , i ,cluster_vs_cluster_stats ,batsman_to_cluster_mapping , bowler_to_cluster_mapping) )
	
	cumulative_pdf = []
	cumulative_pdf.append(pdf[0])
	for i in range(1,len(pdf)):
		cumulative_pdf.append(cumulative_pdf[i-1] + pdf[i])
	
	cumulative_pdf_range = []
	for i in range(0,len(cumulative_pdf)):
		cumulative_pdf_range.append(int(cumulative_pdf[i]*100))
	
	rand_no = random.randint(0,99)
	prediction = get_class(cumulative_pdf_range , rand_no)
	if prediction:
	    return prediction
	else:
	    return 0



def simulate_first_inning(bat_map, bowl_map, cluster_map, batting, bowling):
    print("*"*60)        
    print("Batting : "+batting+", Bowling : "+bowling)
    print("*"*60)
    batsmens = open("teams/"+batting+"/batting_order").readlines()
    wickets_prob = [random.uniform(0.90, 0.96)-x*0.01 for x in range(len(batsmens))]
    batsmens = [Batsmen(batsmens[x].strip(), bat_map[batsmens[x].strip()], wickets_prob[x]) for x in range(len(batsmens))]
    bowlers = open("teams/"+bowling+"/bowling_order").readlines()
    bowlers = [Bowler(x.strip(), bowl_map[x.strip()]) for x in bowlers]
    wickets, overs, score = 0, 0, 0
    onstrike = batsmens.pop(0)
    offstrike = batsmens.pop(0)
    otherbatsmens = [onstrike, offstrike]   
    otherbowlers = []
    while wickets != 10 and overs != 20:
        balls = 0
        currbowler = bowlers.pop(0)
        while wickets != 10 and balls != 6:
            onstrike.increment_balls()
            balls += 1
            if onstrike.is_out():
                wickets += 1
                currbowler.increment_wickets()
                if wickets != 10:
                    onstrike = batsmens.pop(0)
                    otherbatsmens.append(onstrike)
                    
            else:
                run = simulate_ball(onstrike.get_name(), currbowler.get_name(),
                        cluster_map, bat_map, bowl_map)        
                onstrike.add_runs(run)
                onstrike.update_prob()
                currbowler.add_runs(run)
                score += run
                if run in [1, 3, 5, 7]:
                    onstrike, offstrike = offstrike, onstrike
                    
            print("Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
        
        if balls == 6:
            currbowler.increment_overs()
            overs += 1
            random.shuffle(bowlers)
            if currbowler.get_overs() < 4:
                bowlers.append(currbowler)
            else:
                otherbowlers.append(currbowler)        
            
            onstrike, offstrike = offstrike, onstrike
            print("*"*60)
        else:
            currbowler.set_part(balls)
        
        
    print("\n"+"*"*60)        
    print("Innings Scoreboard")
    print("Batting")
    print("{:50}{:3} {}".format("Name", "Runs", "Balls") )
    for bat in otherbatsmens+batsmens:
        print(bat)
        
    print("*"*110)
    print("Bowling")
    print("{:30}{:20}{:20}{:20}{:20}".format("Name", "Overs", "Wickets", "Runs", "Econ"))
    for bowl in set(otherbowlers+bowlers+[currbowler]):
        print(bowl)
    print("*"*110)
    return score
    
def simulate_second_inning(bat_map, bowl_map, cluster_map, batting, bowling, target):
    print("*"*60)
    print("Chasing Target of "+str(target)+" runs.")        
    print("Batting : "+batting+", Bowling : "+bowling)
    print("*"*60)
    batsmens = open("teams/"+batting+"/batting_order").readlines()
    wickets_prob = [random.uniform(0.90, 0.96)-x*0.01 for x in range(len(batsmens))]
    batsmens = [Batsmen(batsmens[x].strip(), bat_map[batsmens[x].strip()], wickets_prob[x]) for x in range(len(batsmens))]
    bowlers = open("teams/"+bowling+"/bowling_order").readlines()
    bowlers = [Bowler(x.strip(), bowl_map[x.strip()]) for x in bowlers]
    wickets, overs, score = 0, 0, 0
    onstrike = batsmens.pop(0)
    offstrike = batsmens.pop(0)
    otherbatsmens = [onstrike, offstrike]   
    otherbowlers = []
    while wickets != 10 and overs != 20 and score <= target:
        balls = 0
        currbowler = bowlers.pop(0)
        while wickets != 10 and balls != 6 and score <= target:
            onstrike.increment_balls()
            balls += 1
            if onstrike.is_out():
                wickets += 1
                currbowler.increment_wickets()
                if wickets != 10:
                    onstrike = batsmens.pop(0)
                    otherbatsmens.append(onstrike)
                    
            else:
                run = simulate_ball(onstrike.get_name(), currbowler.get_name(),
                        cluster_map, bat_map, bowl_map)        
                onstrike.add_runs(run)
                onstrike.update_prob()
                currbowler.add_runs(run)
                score += run
                if run in [1, 3, 5, 7]:
                    onstrike, offstrike = offstrike, onstrike
                
            print("Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
        
        if score >= target:
            break
        if balls == 6:
            currbowler.increment_overs()
            overs += 1
            random.shuffle(bowlers)
            if currbowler.get_overs() < 4:
                bowlers.append(currbowler)
            else:
                otherbowlers.append(currbowler)        
            
            onstrike, offstrike = offstrike, onstrike
            print("*"*60)
        else:
            currbowler.set_part(balls)
    
    print("\n"+"*"*60)        
    print("Innings Scoreboard")
    print("Batting")
    print("{:50}{:3} {}".format("Name", "Runs", "Balls") )
    for bat in otherbatsmens+batsmens:
        print(bat)
        
    print("*"*110)
    print("Bowling")
    print("{:30}{:20}{:20}{:20}{:20}".format("Name", "Overs", "Wickets", "Runs", "Econ"))
    for bowl in set(otherbowlers+bowlers+[currbowler]):
        print(bowl)
    print("*"*110)
    return [score, overs, wickets]

if __name__ == "__main__":
    dump = pickle.load(open("mapping.bin", "rb"))
    bowler_map = dump['bowlmap']
    batsmen_map = dump['batmap']
    cluster_vs_cluster = dump['clustervscluster']
    team1 = input("Enter Team 1 : ")
    team2 = input("Enter Team 2 : ")
    teams = [team1, team2]
    toss = random.choice([0, 1])
    print("*"*60)
    print("Toss won by ", teams[toss], ", Chooses to Bat First!")
    print("*"*60)
    score1 = simulate_first_inning(batsmen_map, bowler_map,
            cluster_vs_cluster, teams[toss], teams[toss-1])
    score2, overs, wickets = simulate_second_inning(batsmen_map, bowler_map,
            cluster_vs_cluster, teams[toss-1], teams[toss], score1)
         
    print("*"*110)    
    if score1 > score2:
        print("Team : "+teams[toss]+", Wins by : "+str(score1-score2)+" runs.")
    elif score2 > score1:
        print("Team : "+teams[toss-1]+", Wins by : "+str(10-wickets)+" wickets.")
    else:
        print("Match Draw")
    print("*"*110)
