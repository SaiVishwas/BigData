import happybase
import pickle
import random

class HBase:
    def __init__(self, host_='localhost', port_=9090, timeout_=10000):
        self.conn = happybase.Connection(host=host_, port=port_, timeout=timeout_)
        
    def terminate(self):
        self.conn.close()
        
    def delete_all_tables(self):
        print("Following tables will be deleted (old tables) :",self.conn.tables())
        for table_name in self.conn.tables():
            self.conn.delete_table(table_name, disable=True)
        
    def create_bowler_table(self):
        self.conn.create_table("Bowler",{"Bowler:GroupNo": dict()})
        
    def create_batsman_table(self):
        self.conn.create_table("Batsman",{"Batsman:GroupNo": dict()})
        
    def create_cluster_table(self):
        self.conn.create_table("Cluster",{"Cluster:GroupNo": dict()})
        
    def get_bowler_group(self, name):
        table = self.conn.table("Bowler")
        row = table.row(name.encode("utf-8"))
        return int(row["Bowler:GroupNo".encode('utf-8')])
        
    def get_batsman_group(self, name):
        table = self.conn.table("Batsman")
        row = table.row(name.encode("utf-8"))
        return int(row["Batsman:GroupNo".encode('utf-8')])
        
    def get_cluster_stats(self, Gbat, Gbowl):
        name = str(Gbat)
        table = self.conn.table("Cluster")
        row = table.row(name.encode("utf-8"))
        tmp = row["Cluster:GroupNo".encode('utf-8')]
        tmp = tmp.decode('utf-8').split(";")[Gbowl]
        return [int(x) for x in tmp.split(":")]
 
    def add_bowlers(self, maps):
        table = self.conn.table("Bowler")
        for key,value in maps.items():
            table.put(key, {"Bowler:GroupNo": str(value)}) 
        
    def add_batsmans(self, maps):
        table = self.conn.table("Batsman")
        for key,value in maps.items():
            table.put(key, {"Batsman:GroupNo": str(value)})
            
    def add_clusters(self, maps):
        table = self.conn.table("Cluster")
        for key,value in maps.items():
            table.put(key, {"Cluster:GroupNo": str(value)})

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
        
        
def get_probability_of_run(batsman_name , bowler_name , runs) :
	bat_cluster_no = hbase.get_batsman_group(batsman_name)
	bowl_cluster_no = hbase.get_bowler_group(bowler_name)
	stats = hbase.get_cluster_stats(bat_cluster_no, bowl_cluster_no)
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
			
def simulate_ball(batsman_name , bowler_name):
	pdf = []
	for i in range(8):
		pdf.append(get_probability_of_run(batsman_name, bowler_name , i))
	
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



def simulate_first_inning(batting, bowling):
    print("*"*60)        
    print("Batting : "+batting+", Bowling : "+bowling)
    print("*"*60)
    batsmens = open("teams/"+batting+"/batting_order").readlines()
    wickets_prob = sorted([random.uniform(0.92, 0.98)-x*0.01 for x in range(len(batsmens))], reverse=True)
    batsmens = [Batsmen(batsmens[x].strip(), hbase.get_batsman_group(batsmens[x].strip()), wickets_prob[x]) for x in range(len(batsmens))]
    bowlers = open("teams/"+bowling+"/bowling_order").readlines()
    bowlers = [Bowler(x.strip(), hbase.get_bowler_group(x.strip())) for x in bowlers]
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
                print(currbowler.get_name()+" to "+onstrike.get_name()+" : OUT!!!, Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
                currbowler.increment_wickets()
                if wickets != 10:
                    onstrike = batsmens.pop(0)
                    otherbatsmens.append(onstrike)
                    
            else:
                run = simulate_ball(onstrike.get_name(), currbowler.get_name())        
                onstrike.add_runs(run)
                onstrike.update_prob()
                currbowler.add_runs(run)
                score += run
                print(currbowler.get_name()+" to "+onstrike.get_name()+", Run scored : "+str(run)+", Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
                if run in [1, 3, 5, 7]:
                    onstrike, offstrike = offstrike, onstrike
                    
        
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
    
def simulate_second_inning(batting, bowling, target):
    print("*"*60)
    print("Chasing Target of "+str(target)+" runs.")        
    print("Batting : "+batting+", Bowling : "+bowling)
    print("*"*60)
    batsmens = open("teams/"+batting+"/batting_order").readlines()
    wickets_prob = sorted([random.uniform(0.92, 0.98)-x*0.01 for x in range(len(batsmens))], reverse=True)
    batsmens = [Batsmen(batsmens[x].strip(), hbase.get_batsman_group(batsmens[x].strip()), wickets_prob[x]) for x in range(len(batsmens))]
    bowlers = open("teams/"+bowling+"/bowling_order").readlines()
    bowlers = [Bowler(x.strip(), hbase.get_bowler_group(x.strip())) for x in bowlers]
    wickets, overs, score = 0, 0, 0
    onstrike = batsmens.pop(0)
    offstrike = batsmens.pop(0)
    otherbatsmens = [onstrike, offstrike]   
    otherbowlers = []
    while wickets != 10 and overs != 20 and score <= target:
        balls = 0
        currbowler = bowlers.pop(0)
        while wickets != 10 and balls != 6 and score <= target:
            run = "Wicket"
            onstrike.increment_balls()
            balls += 1
            if onstrike.is_out():
                wickets += 1
                print(currbowler.get_name()+" to "+onstrike.get_name()+" : OUT!!!, Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
                currbowler.increment_wickets()
                if wickets != 10:
                    onstrike = batsmens.pop(0)
                    otherbatsmens.append(onstrike)
                    
            else:
                run = simulate_ball(onstrike.get_name(), currbowler.get_name())        
                onstrike.add_runs(run)
                onstrike.update_prob()
                currbowler.add_runs(run)
                score += run
                print(currbowler.get_name()+" to "+onstrike.get_name()+", Run scored : "+str(run)+", Overs :"+str(overs)+"."+str(balls)+", "+str(score)+"/"+str(wickets))
                if run in [1, 3, 5, 7]:
                    onstrike, offstrike = offstrike, onstrike
            
        
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
    hbase = HBase()
    #team1 = input("Enter Team 1 : ")
    #team2 = input("Enter Team 2 : ")
    res = dict()
    all_teams = ["MI", "RCB", "KKR", "DD", "CSK", "SRH", "KXIP", "RR"]
    for t in all_teams:
        res[t] = 0
    for team1 in all_teams:
        for team2 in all_teams:
            teams = [team1, team2]
            toss = random.choice([0, 1])
            print("*"*60)
            print("Toss won by ", teams[toss], ", Chooses to Bat First!")
            print("*"*60)
            score1 = simulate_first_inning(teams[toss], teams[toss-1])
            score2, overs, wickets = simulate_second_inning(teams[toss-1], teams[toss], score1)
                 
            print("*"*110)    
            if score1 > score2:
                print("Team : "+teams[toss]+", Wins by : "+str(score1-score2)+" runs.")
                res[teams[toss]] += 1
            elif score2 > score1:
                print("Team : "+teams[toss-1]+", Wins by : "+str(10-wickets)+" wickets.")
                res[teams[toss-1]] += 1
            else:
                res[teams[toss]] += 1
                print("Match Draw")
            print("*"*110)
            
        
    for k,v in res.items():
        print(k, ":", v)
