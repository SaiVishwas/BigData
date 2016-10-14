import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)

  tokenized = sc.textFile("batsman_input/batsmen.csv").flatMap(lambda line: line.split("\n"))
  output = open("output.txt", 'w')
  
  stat = tokenized.map(lambda entry: (entry, [float(entry.split(",")[7]),float(entry.split(",")[9]),int(entry.split(",")[8]),0]))
  
  avg = []
  sr = []
  bf = []
  
  for i in stat.collect() :
    #i = [map(int, ele) for ele in i[1] ]
    #i[1][0] = 0
    avg.append(i[1][0])
    sr.append(i[1][1])
    bf.append(i[1][2])        
    
  #output.write(repr(avg) + '\n' + repr(sr) + '\n' + repr(bf) )
  
  sorted_avg = sorted(avg)
  sorted_sr = sorted(sr)
  sorted_bf = sorted(bf)
  
  #output.write(repr(sorted_avg) + '\n' + repr(sorted_sr) + '\n' + repr(sorted_bf) + '\n') 
  
  avg_splitpoints = []
  sr_splitpoints = []
  bf_splitpoints = []

  avg_splitpoints.append(sorted_avg[len(sorted_avg)/2])
  
  sr_splitpoints.append(sorted_sr[len(sorted_sr)/4])
  sr_splitpoints.append(sorted_sr[len(sorted_sr)*3/4])   
  
  bf_splitpoints.append(sorted_bf[len(sorted_bf)/8])
  bf_splitpoints.append(sorted_bf[len(sorted_bf)*3/8])
  bf_splitpoints.append(sorted_bf[len(sorted_bf)*5/8])
  bf_splitpoints.append(sorted_bf[len(sorted_bf)*7/8])
 
  output.write(repr(avg_splitpoints) + '\n' + repr(sr_splitpoints) + '\n' + repr(bf_splitpoints) ) 
  
  
  
  for i in stat.collect() :
    #output.write('0')
    if i[1][0] < avg_splitpoints[0] :
      if i[1][1] < sr_splitpoints[0] :
        if i[1][2] < bf_splitpoints[0] :
          i[1][3] = 1
          output.write('1')
        else :
          i[1][3] = 2
          output.write('2')
      else : 
        if i[1][2] < bf_splitpoints[1] :
          i[1][3] = 3
          output.write('3')
        else :
          i[1][3] = 4
          output.write('4')
    else :
      if i[1][1] < sr_splitpoints[1] :
        if i[1][2] < bf_splitpoints[2] :
          i[1][3] = 5
          output.write('5')
        else :
          i[1][3] = 6
          output.write('6')
      else : 
        if i[1][2] < bf_splitpoints[3] :
          i[1][3] = 7
          output.write('7')
        else :
          i[1][3] = 8      
          output.write('8')

  

    
  
  #output.write(repr(stat.collect())[:])
  #output.write(repr(stat.collect()))

  
  
