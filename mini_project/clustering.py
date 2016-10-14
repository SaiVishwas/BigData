import sys

from pyspark import SparkContext, SparkConf


def get_split_points(input_file , indices):
  
  tokenized = sc.textFile(input_file).flatMap(lambda line: line.split("\n"))
  stat = tokenized.map(lambda entry: (entry, [float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),int(entry.split(",")[indices[2]]),0]))

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




if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  output = open("output_1.txt", 'w')

  avg_splitpoints , sr_splitpoints , bf_splitpoints = get_split_points("batsman_input/batsmen.csv" , [7,9,8])
  output.write('\n' + repr(avg_splitpoints) + '\n' + repr(sr_splitpoints) + '\n' + repr(bf_splitpoints) + '\n\n')


  


