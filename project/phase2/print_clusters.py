from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from collections import defaultdict

def format_input(input_file , indices) :
  tokenized = sc.textFile(input_file).flatMap(lambda line: line.split("\n"))
  stat = tokenized.map(lambda entry: (entry.split(",")[0] ,[float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  data = tokenized.map(lambda entry: ([float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  return [stat, data]

def cluster(filename, k, indices):
  stat, data = format_input("input/"+filename, indices)
  model = KMeans.train(data, k)
  cluster_centers = model.clusterCenters
  clusters = defaultdict(list)

  for x in stat.collect():
    name = str(x[0])
    num = int(model.predict(x[1]))
    centers = [i for i in cluster_centers[model.predict(x[1])]]
    clusters[num].append(name)
    
  return clusters,cluster_centers    

def print_results(filename , clusters , cluster_centers):
	with open("output_clusters/"+filename, "w") as f:
		for i in clusters :
			f.write("Cluster number : " + str(i) + "\n")

			for j in clusters[i]:
				f.write(j + "\n")

			f.write("Cluster centers : " + str(cluster_centers[i]) + "\n\n")	


if __name__ == "__main__":
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  batsman_cluster , batsman_cluster_centers = cluster('batsmen', 8, [7, 9, 8])
  bowler_cluster , bowler_cluster_centers = cluster('bowler', 8, [11,10,4])

  print_results('batsmen' , batsman_cluster , batsman_cluster_centers)
  print_results('bowler' , bowler_cluster , bowler_cluster_centers)
  
  import pickle
  #favorite_color = { "lion": "yellow", "kitty": "red" }
  pickle.dump(  batsman_cluster , open( "batsman_clusters.p", "wb" ) )
  pickle.dump(  bowler_cluster , open( "bowler_clusters.p", "wb" ) )  


  sc.stop()
