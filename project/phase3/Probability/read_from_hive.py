'''
from pyspark import HiveContext
from pyspark.context import SparkContext

#rdd = HiveContext(sc).sql('from batsman select *')

#print rdd

sc = SparkContext()
hive_context = HiveContext(sc)
bank = hive_context.table("cluster.bowler")
#bank.show()

bank.registerTempTable("cluster.bowler")
out = hive_context.sql("select * from bowler;")

print(out)


from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext
#conf = SparkConf().setAppName('inc_dd_openings')
sc = SparkContext()
sqlContext = HiveContext(sc)

df_openings_latest = sqlContext.sql('select * from cluster.bowler')

print df_openings_latest


'''


from collections import defaultdict

from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext
sc = SparkContext()
sqlContext = HiveContext(sc)

#sqlContext.sql(" create table if not exists batsman2 (name string , cluster int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
#sqlContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/BigData/project/phase2/output/batsmen' INTO TABLE batsman2")
sqlContext.sql("use ipl")
a = sqlContext.sql("select * from batsmen")
#a = sqlContext.sql("select * from batsman2")
#x = a.map(lambda p: p.name).collect()

x = a.rdd.map(lambda p: str(p.name) + "," + str(p.cluster)).collect()
print(x)
#print type(x)
clusters = defaultdict(list)

for i in x :
	#print i
	name = i.split(",")[0]
	cluster = i.split(",")[1]
	clusters[cluster].append(name)

print(clusters)	

#sqlContext.sql(" create table if not exists bowler2 (name string , cluster int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
#sqlContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/BigData/project/phase2/output/bowler' INTO TABLE bowler2")
b = sqlContext.sql("select * from bowler")

#print(str(b.collect()))
#y = b.map(lambda p: p.name).collect()
#print b
