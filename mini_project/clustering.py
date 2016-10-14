import sys

from pyspark import SparkContext, SparkConf

def get_stats(input_file , indices) :
  tokenized = sc.textFile(input_file).flatMap(lambda line: line.split("\n"))
  stat = tokenized.map(lambda entry: (entry, [float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]]),0]))
  return stat

def get_split_points(stat):
  
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
  
  first_splitpoints = []
  second_splitpoints = []
  third_splitpoints = []
  

  split_info = sorted(split_info , key = lambda s : s[0] )

  #output.write(repr(split_info))  

  first_splitpoints.append(split_info[len(split_info)/2][0])
  
  split_info_1  = split_info[:len(split_info)/2]
  split_info_2 = split_info[len(split_info)/2 : ]

  split_info_1 = sorted(split_info_1 , key = lambda s : s[1])
  split_info_2 = sorted(split_info_2 , key = lambda s : s[1])

  second_splitpoints.append(split_info_1[len(split_info_1)/2][1])
  second_splitpoints.append(split_info_2[len(split_info_2)/2][1])

  split_info_a = split_info_1[:len(split_info_1)/2]
  split_info_b = split_info_1[len(split_info_1)/2:]
  
  split_info_c = split_info_2[:len(split_info_2)/2]
  split_info_d = split_info_2[len(split_info_2)/2:]

  split_info_a = sorted(split_info_a, key = lambda s : s[2])
  split_info_b = sorted(split_info_b, key = lambda s : s[2])
  split_info_c = sorted(split_info_c, key = lambda s : s[2])
  split_info_d = sorted(split_info_d, key = lambda s : s[2])
  
  third_splitpoints.append(split_info_a[len(split_info_a)/2][2])
  third_splitpoints.append(split_info_b[len(split_info_b)/2][2])
  third_splitpoints.append(split_info_c[len(split_info_c)/2][2])
  third_splitpoints.append(split_info_d[len(split_info_d)/2][2])

  return first_splitpoints , second_splitpoints , third_splitpoints


def get_cluster_no( stat , first_splitpoints , second_splitpoints , third_splitpoints ):
  cluster_no = 0
  if stat[0] < first_splitpoints[0] :
    if stat[1] < second_splitpoints[0] :
      if stat[2] < third_splitpoints[0] :
        cluster_no = 1
      else :
        cluster_no = 2
    else : 
      if stat[2] < third_splitpoints[1] :
        cluster_no = 3
      else :
        cluster_no = 4
  else :
    if stat[1] < second_splitpoints[1] :
      if stat[2] < third_splitpoints[2] :
        cluster_no = 5
      else :
        cluster_no = 6
    else : 
      if stat[2] < third_splitpoints[3] :
        cluster_no = 7
      else :
        cluster_no = 8      

  return cluster_no     



if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  output = open("output_1.txt", 'w')

  batsmen_stat = get_stats("batsman_input/batsmen.csv" , [7,9,8])
  bat_avg_splitpoints , bat_sr_splitpoints , bat_bf_splitpoints = get_split_points(batsmen_stat)
  #output.write('\n' + repr(bat_avg_splitpoints) + '\n' + repr(bat_sr_splitpoints) + '\n' + repr(bat_bf_splitpoints) + '\n\n')

  clusters = []

  for i in batsmen_stat.collect() : 
    #output.write(repr(i))
    cluster_no = get_cluster_no(i[1] , bat_avg_splitpoints , bat_sr_splitpoints , bat_bf_splitpoints)
    #output.write(repr(cluster_no))
    clusters.append([str(i[0].split(",")[0]) , [cluster_no]])


  bowler_stat = get_stats("bowler_input/bowlers.csv" , [11,10,4])
  bowl_sr_splitpoints , bowl_economy_splitpoints , bowl_avg_splitpoints = get_split_points(bowler_stat)
  #output.write('\n' + repr(bowl_sr_splitpoints) + '\n' + repr(bowl_economy_splitpoints) + '\n' + repr(bowl_avg_splitpoints) + '\n\n')

  count = 0
  for i in bowler_stat.collect() : 
    #output.write(repr(i))
    cluster_no = get_cluster_no(i[1] , bowl_sr_splitpoints , bowl_economy_splitpoints , bowl_avg_splitpoints)
    #output.write(repr(cluster_no))
    #clusters.append([str(i[0].split(",")[0]) , [cluster_no]])  
    clusters[count][1].append(cluster_no)
    count += 1

  #output.write(repr(clusters))  

  for i in clusters :
    output.write(i[0] + '\t' + str(i[1][0]) + '\t' + str(i[1][1]) + '\n')
    #output.write(str(i[1][1]))
  


