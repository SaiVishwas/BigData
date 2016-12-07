import pickle

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
        
    for k in sorted(cluster_map.keys()):
        print(k)
        for m in cluster_map[k].split(";"):
            print(m)
            
        print("\n\n")
