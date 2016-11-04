from pyspark import SparkContext, SparkConf
import csv

conf = SparkConf().setAppName('PlayerVSPlayer')
sc = SparkContext(conf=conf)

import timeit
import time
start_time = timeit.default_timer()
st=time.ctime()

read=sc.textFile("PlayerVsPlayer.csv")
token=read.flatMap(lambda line: line.split("\n")).map(lambda w: w.split(","))
#Nrecords=token.count()
Nrecords=token.count()

batsman=[]
bats=0
bowls=0

bowler=[]
d=dict()
i=0
while i < Nrecords-1:
	line1=token.take(Nrecords)[i][0]
	if(line1 <> "" and token.take(Nrecords)[i+1][0].find("v Bowler") <> -1):     # take care of null lines
		if(line1.find("\"") <> -1):
			batsman.append(line1.split("-")[0].split("\"")[1])     # assumes Batsman name will have a single double quote
		else:
			batsman.append(line1.split("-")[0])     # Batsman name doesnot have  double quote
		bats+=1
 		#print i, line1  # Batsman Name
	j=i+2
	line2=token.take(Nrecords)[j][0]
	while (line2 <> "" and line2.find("innings")	== -1 and line2.find("v Bowler")	== -1 ):        # take care of null lines
		#print j, line2 # Bowler Name
		bowler.append(line2)
		bowls+=1
		bt=str(batsman[bats-1])  # casting to string
		bw=str(bowler[bowls-1])  # casting to string
		if(d.has_key((batsman[bats-1],bowler[bowls-1],0))):
 			d[(bt,bw,0)]+=int(token.take(Nrecords)[j][1])
			d[(bt,bw,1)]+=int(token.take(Nrecords)[j][2])
			d[(bt,bw,2)]+=int(token.take(Nrecords)[j][3])
			d[(bt,bw,3)]+=int(token.take(Nrecords)[j][4])
			d[(bt,bw,4)]+=int(token.take(Nrecords)[j][5])
			d[(bt,bw,5)]+=int(token.take(Nrecords)[j][6])
			d[(bt,bw,6)]+=int(token.take(Nrecords)[j][7])
			d[(bt,bw,7)]+=int(token.take(Nrecords)[j][8])
			d[(bt,bw,8)]+=int(token.take(Nrecords)[j][10]) 	#runs
			d[(bt,bw,9)]+=int(token.take(Nrecords)[j][11])	#balls
		else:
 			d[(bt,bw,0)]=int(token.take(Nrecords)[j][1])
			d[(bt,bw,1)]=int(token.take(Nrecords)[j][2])
			d[(bt,bw,2)]=int(token.take(Nrecords)[j][3])
			d[(bt,bw,3)]=int(token.take(Nrecords)[j][4])
			d[(bt,bw,4)]=int(token.take(Nrecords)[j][5])
			d[(bt,bw,5)]=int(token.take(Nrecords)[j][6])
			d[(bt,bw,6)]=int(token.take(Nrecords)[j][7])
			d[(bt,bw,7)]=int(token.take(Nrecords)[j][8])
			d[(bt,bw,8)]=int(token.take(Nrecords)[j][10])
			d[(bt,bw,9)]=int(token.take(Nrecords)[j][11])
		j+=1
		if(j < Nrecords) :
			line2=token.take(Nrecords)[j][0]
		else :
			break

	i=j-1
finalDict = dict()
tempDict = dict()
templ = list()
for key, value in d.items():
	templist = [0,0,0,0,0,0,0,0,0,0]
	for key1, value1 in d.items():
		if(key1[0] == key[0] and key1[1] == key[1]):
			templist[key1[2]] = value1
	#print(key[0], key[1], templist)
	templ.append([key[0], key[1], templist])

for i in templ:		# can be made efficient , check the finalDict if the batsman is already present, then dont compute again
	for j in templ:
		if i[0] == j[0]:
			tempDict[j[1]] = j[2]
	finalDict[i[0]] = tempDict
	tempDict = dict()

#for i,j in finalDict.items():
#	print(i,j)
			
#tokenized = sc.textFile("batsmen.csv").flatMap(lambda line: line.split("\n"))

#for i in tokenized:

 # Save a dictionary into a pickle file.
import pickle
#favorite_color = { "lion": "yellow", "kitty": "red" }
pickle.dump( finalDict, open( "save.p", "wb" ) )

with open('final1.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for key, value in finalDict.items():
		writer.writerow([key, value])

et=time.ctime()
elapsed = timeit.default_timer() - start_time
print "\n\n===================\n", "Num of Records = ", Nrecords, "\n===================\n"
print "\n\n===================\n", "Started at ", st, "\nEnded at ", et, "\nElapsed Time (s) = ", elapsed, "\n===================\n"
