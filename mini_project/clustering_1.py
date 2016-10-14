import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)

  tokenized = sc.textFile("batsman_input/batsmen.csv").flatMap(lambda line: line.split("\n"))
  output = open("output.txt", 'w')
  
  stat = tokenized.map(lambda entry: (entry, [float(entry.split(",")[7]),float(entry.split(",")[9]),int(entry.split(",")[8]),0]))
  
  split_info = []
  
  for i in stat.collect() :
    #i = [map(int, ele) for ele in i[1] ]
    #i[1][0] = 0
    tmp = [] 
    tmp.append(i[1][0])
    tmp.append(i[1][1])
    tmp.append(i[1][2])        
    
    split_info.append(tmp)
    
  #output.write(repr(split_info))
  
  avg_splitpoints = []
  sr_splitpoints = []
  bf_splitpoints = []
  

  split_info = sorted(split_info , key = lambda s : s[0] )

  #output.write(repr(split_info))  

  avg_splitpoints.append(split_info[len(split_info)/2][0])
  
  split_info_1  = split_info[:len(split_info)/2]
  split_info_5 = split_info[len(split_info)/2 : ]

  split_info_1 = sorted(split_info_1 , key = lambda s : s[1])
  split_info_5 = sorted(split_info_5 , key = lambda s : s[1])

  sr_splitpoints.append(split_info_1[len(split_info_1)/2][1])
  sr_splitpoints.append(split_info_5[len(split_info_5)/2][1])

  split_info_a = split_info_1[:len(split_info_1)/2]
  split_info_b = split_info_1[len(split_info_1)/2:]
  
  split_info_c = split_info_5[:len(split_info_5)/2]
  split_info_d = split_info_5[len(split_info_5)/2:]

  split_info_a = sorted(split_info_a, key = lambda s : s[2])
  split_info_b = sorted(split_info_b, key = lambda s : s[2])
  split_info_c = sorted(split_info_c, key = lambda s : s[2])
  split_info_d = sorted(split_info_d, key = lambda s : s[2])
  
  bf_splitpoints.append(split_info_a[len(split_info_a)/2][2])
  bf_splitpoints.append(split_info_b[len(split_info_b)/2][2])
  bf_splitpoints.append(split_info_c[len(split_info_c)/2][2])
  bf_splitpoints.append(split_info_d[len(split_info_d)/2][2])

  #output.write('\n' + repr(split_info_1) + '\n' + repr(split_info_5))  

  #output.write('\n' + repr(avg_splitpoints) + '\n' + repr(sr_splitpoints) + '\n' + repr(bf_splitpoints) + '\n\n')

  clusters = []

  for i in stat.collect() :
    #output.write('0')
    cluster_no = 0
    if i[1][0] < avg_splitpoints[0] :
      if i[1][1] < sr_splitpoints[0] :
        if i[1][2] < bf_splitpoints[0] :
          cluster_no = 1
        else :
          cluster_no = 2
      else : 
        if i[1][2] < bf_splitpoints[1] :
          cluster_no = 3
        else :
          cluster_no = 4
    else :
      if i[1][1] < sr_splitpoints[1] :
        if i[1][2] < bf_splitpoints[2] :
          cluster_no = 5
        else :
          cluster_no = 6
      else : 
        if i[1][2] < bf_splitpoints[3] :
          cluster_no = 7
        else :
          cluster_no = 8      

    clusters.append([str(i[0].split(",")[0]) , [cluster_no]])


  output.write(repr(clusters))  
    
  '''  
  output.write(repr(avg) + '\n' + repr(sr) + '\n' + repr(bf) )
  
  sorted_avg = sorted(avg)
  sorted_sr = sorted(sr)
  sorted_bf = sorted(bf)
  
  output.write(repr(sorted_avg) + '\n' + repr(sorted_sr) + '\n' + repr(sorted_bf) + '\n') 
  
  avg_splitpoints = []
  sr_splitpoints = []
  bf_splitpoints = []
  '''


