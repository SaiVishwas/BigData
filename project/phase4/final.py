import happybase
import pickle

class HBase:
    def __init__(self, host_='localhost', port_=9090, timeout_=100000):
        self.conn = happybase.Connection(host=host_, port=port_)
        
    def terminate(self):
        self.conn.terminate()
        
    def create_bowler_table(self):
        self.conn.create_table("Bowler",{"Bowler:GroupNo": dict()})
        
    def create_batsman_table(self):
        self.conn.create_table("Batsman",{"Batsman:GroupNo": dict()})
        
    def get_bowler_group(self, name):
        row = table.row(name.encode("utf-8")
        return int(row["Bowler:GroupNo".encode('utf-8')])
        
    def get_batsman_group(self, name):
        row = table.row(name.encode("utf-8")
        return int(row["Batsman:GroupNo".encode('utf-8')])
 
    def add_bowlers(self, maps):
        for key,value in maps.items():
            table.put(key, {"Bowler:GroupNo": str(value)}) 
        
    def add_batsmans(self, maps):
        for key,value in maps.items():
            table.put(key, {"Batsman:GroupNo": str(value)})        
        

def main(table_name, maps):
    connection = happybase.Connection(host='localhost', port=9090, timeout=10000)

    try:
        print('Creating the {} table.'.format(table_name))
        column_family_name = 'Group'
        connection.create_table(
            table_name,
            {
                column_family_name: dict()
            })

        print('Writing some greetings to the table.')
        table = connection.table(table_name)
        column_name = '{fam}:No'.format(fam=column_family_name)
        '''
        maps = {
            "Sachin Tendulkar":"1",
            "Rahul Dravid":"2",
            "VVS Laxman":"2"
        }
        '''
        for i, value in maps.items():
            row_key = '{}'.format(i)
            print(i, value)
            table.put(i, {column_name: value})
 
        for key, row in table.scan():
            print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))

        print('Deleting the {} table.'.format(table_name))
        connection.delete_table(table_name, disable=True)

    finally:
        connection.close()


if __name__ == '__main__':
    dump = pickle.load(open("mapping.bin", "rb"))
    bowler_map = dump['bowlmap']
    for k in bowler_map.keys():
        bowler_map[k] = str(bowler_map[k])
    #print(bowler_map)
    #main("newTable", bowler_map)
    h = HBase()
    #h.create_table("Bowler", ["Name", "GroupNo"])
    h.terminate()
