from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans

def format_input(input_file , indices) :
  tokenized = sc.textFile(input_file).flatMap(lambda line: line.split("\n"))
  stat = tokenized.map(lambda entry: (entry.split(",")[0] ,[float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  data = tokenized.map(lambda entry: ([float(entry.split(",")[indices[0]]),float(entry.split(",")[indices[1]]),float(entry.split(",")[indices[2]])]))
  return [stat, data]

def cluster(filename, k, indices):
  stat, data = format_input("input/"+filename, indices)
  output = file("output/"+filename, "w") 
  model = KMeans.train(data, k)
  cluster_centers = model.clusterCenters
  with file("output/"+filename, "w") as f:
      for x in stat.collect():
        name = str(x[0])
        num = str(model.predict(x[1]))
        centers = str(' '.join('{:.3f}'.format(i) for i in cluster_centers[model.predict(x[1])]))
        #f.write(name+" : "+num+ " : "+centers+"\n")
        f.write(name+","+num+"\n")

if __name__ == "__main__":
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  cluster('batsmen', 8, [7, 9, 8])
  cluster('bowler', 8, [11,10,4])
  sc.stop()
