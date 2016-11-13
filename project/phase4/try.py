import happybase
import pickle

def main(table_name, maps):
    connection = happybase.Connection(host='localhost', port=9090, timeout=10000)

    try:
        print('Creating the {} table.'.format(table_name))
        column_family_name = 'Group:No'
        connection.create_table(
            table_name,
            {
                column_family_name: dict()
            })
        
        print('Writing some greetings to the table.')
        table = connection.table(table_name)
        column_name = '{fam}'.format(fam=column_family_name)
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
    print(bowler_map)
    main("newTable", bowler_map)
