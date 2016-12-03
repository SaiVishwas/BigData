import happybase
import pickle

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

if __name__ == '__main__':   
    dump = pickle.load(open("mapping.bin", "rb"))
    bowler_map = dump['bowlmap']
    for k in bowler_map.keys():
        bowler_map[k] = str(bowler_map[k])
    batsman_map = dump['batmap']
    for k in bowler_map.keys():
        batsman_map[k] = str(batsman_map[k])
    cluster_vs_cluster = dump['clustervscluster']
    cluster_map = dict()
    for i in range(len(cluster_vs_cluster)):
        clust = cluster_vs_cluster[i]
        clust = [':'.join(str(i) for i in list(c)) for c in clust]
        cluster_map[str(i)] = ';'.join(clust)
    
    try:
        hbase = HBase()
        hbase.delete_all_tables()
        hbase.create_bowler_table()
        hbase.create_batsman_table()
        hbase.create_cluster_table()
        print("Bowler, Batsman and Cluster tables created")
        hbase.add_clusters(cluster_map)
        hbase.add_bowlers(bowler_map)
        hbase.add_batsmans(batsman_map)    
        print("Data inserted in Bowler, Batsman and Cluster tables")
    finally:
        hbase.terminate()
