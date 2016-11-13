from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from pyspark.sql import HiveContext

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

def dump_to_hive(batsmen_file, bowler_file):
  sqlContext = HiveContext(sc)
  sqlContext.sql("DROP DATABASE IF EXISTS ipl")
  sqlContext.sql("CREATE DATABASE ipl")
  sqlContext.sql("USE ipl")

  sqlContext.sql("CREATE TABLE IF NOT EXISTS batsmen (name string , cluster int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
  sqlContext.sql("LOAD DATA LOCAL INPATH 'output/" + batsmen_file + "' INTO TABLE batsmen")
  #sqlContext.sql("select * from batsmen").show()

  sqlContext.sql("CREATE TABLE IF NOT EXISTS bowler (name string , cluster int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
  sqlContext.sql("LOAD DATA LOCAL INPATH 'output/" + bowler_file + "' INTO TABLE bowler")
  #sqlContext.sql("select * from bowler").show()    

if __name__ == "__main__":
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  cluster('batsmen', 8, [7, 9, 8])
  cluster('bowler', 8, [11,10,4])
  dump_to_hive('batsmen', 'bowler')
  sc.stop()
